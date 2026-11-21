
"""Part E3.2 — Dynamic worker join and graceful leave over gRPC."""

from concurrent import futures
from math import isclose

import grpc
import manager_pb2 as manager_pb
import manager_pb2_grpc as manager_grpc

from harness.local_servers import make_server
from server_manager import ServerManager
from manager_rpc import ManagerService
from distributed_client import DistributedParameterClient
from coordinated_client import CoordinatedClient
from routing import Partition, KEY_SPACE_END


def run_iteration(worker, iteration):
    """Run one coordinated Push without waiting for the next turn."""

    worker.wait_for_turn(iteration)

    task = worker.push(
        [10],
        [1.0, 1.0],
        width=2,
    )

    assert task.wait(timeout_ms=5000).accepted

    worker.iteration_finished(
        iteration,
        wait_for_next=False,
    )


def main():
    server, service, address = make_server()

    manager = ServerManager([
        Partition("p1", 0, KEY_SPACE_END, address)
    ])

    manager.configure_consistency(
        workers=[1101, 1102],
        mode="sequential",
    )

    service.set_ownership(address, manager.get_map())
    server.start()

    grpc_manager = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4)
    )

    manager_grpc.add_OwnershipManagerServicer_to_server(
        ManagerService(manager),
        grpc_manager,
    )

    port = grpc_manager.add_insecure_port("127.0.0.1:0")
    assert port != 0

    grpc_manager.start()
    manager_address = f"127.0.0.1:{port}"

    workers = {}
    channel = grpc.insecure_channel(manager_address)
    stub = manager_grpc.OwnershipManagerStub(channel)

    def create_worker(worker_id):
        distributed = DistributedParameterClient.from_manager(
            manager_address,
            worker_id=worker_id,
            value_width=2,
        )

        worker = CoordinatedClient(
            distributed,
            manager_address,
            worker_id=worker_id,
            total_iterations=3,
        )

        workers[worker_id] = worker
        return worker

    try:
        # ---------------------------------------------
        # 1. Begin training with two workers
        # ---------------------------------------------

        worker_a = create_worker(1101)
        worker_b = create_worker(1102)

        run_iteration(worker_a, 1)
        run_iteration(worker_b, 1)

        assert manager.consistency_gate.snapshot() == {
            1101: 1,
            1102: 1,
        }

        print("Initial workers completed iteration 1: PASS")

        # ---------------------------------------------
        # 2. Join a third worker during training
        # ---------------------------------------------

        worker_c = create_worker(1103)

        starting_iteration = worker_c.join_training()

        assert starting_iteration == 2
        assert worker_c.iteration == 2
        assert manager.consistency_gate.membership_epoch == 2

        assert manager.consistency_gate.active_snapshot() == {
            1101: 1,
            1102: 1,
            1103: 1,
        }

        print("New worker joined at iteration 2: PASS")
        print("Membership epoch incremented: PASS")

        # The newcomer can read already-trained parameters.
        initial_values = worker_c.pull(
            [10]
        ).wait(timeout_ms=5000).values

        assert all(
            isclose(v, -0.2, abs_tol=1e-9)
            for v in initial_values
        )

        print("Joined worker reads existing parameters: PASS")

        # ---------------------------------------------
        # 3. Existing and new workers continue training
        # ---------------------------------------------

        run_iteration(worker_a, 2)
        run_iteration(worker_c, 2)

        # Worker B is still at iteration 1.
        # It must block A from starting iteration 3
        # under sequential consistency.
        assert not manager.consistency_gate.can_start(
            3, worker_id=1101
        )

        print("Sequential barrier respected after join: PASS")

        # ---------------------------------------------
        # 4. Worker B leaves gracefully
        # ---------------------------------------------

        last_iteration = worker_b.leave_training()

        assert last_iteration == 1
        assert manager.consistency_gate.membership_epoch == 3

        assert manager.consistency_gate.active_snapshot() == {
            1101: 2,
            1103: 2,
        }

        # B's historical progress must remain stored.
        assert manager.consistency_gate.snapshot()[1102] == 1

        print("Worker B left gracefully: PASS")
        print("Departed worker progress preserved: PASS")

        # B no longer blocks iteration 3.
        assert manager.consistency_gate.can_start(
            3, worker_id=1101
        )

        print("Departure unblocks consistency barrier: PASS")

        # A departed worker must not submit more work.
        try:
            worker_b.push(
                [10], [1.0, 1.0], width=2
            )
        except RuntimeError:
            pass
        else:
            raise AssertionError(
                "Departed worker submitted a Push"
            )

        print("Departed worker cannot submit Push: PASS")

        # Manager must also reject its old membership.
        try:
            stub.CanStart(
                manager_pb.CanStartRequest(
                    iteration=2,
                    worker_id=1102,
                ),
                timeout=5.0,
            )
        except grpc.RpcError as error:
            assert (
                error.code()
                == grpc.StatusCode.INVALID_ARGUMENT
            )
        else:
            raise AssertionError(
                "Manager accepted a departed worker"
            )

        print("Manager rejects departed worker: PASS")

        # Historical worker IDs cannot be reused.
        try:
            stub.JoinWorker(
                manager_pb.JoinWorkerRequest(worker_id=1102),
                timeout=5.0,
            )
        except grpc.RpcError as error:
            assert (
                error.code()
                == grpc.StatusCode.INVALID_ARGUMENT
            )
        else:
            raise AssertionError(
                "Departed worker ID was reused"
            )

        print("Historical worker ID reuse rejected: PASS")

        # ---------------------------------------------
        # 5. Remaining workers finish iteration 3
        # ---------------------------------------------

        run_iteration(worker_a, 3)
        run_iteration(worker_c, 3)

        assert manager.consistency_gate.active_snapshot() == {
            1101: 3,
            1103: 3,
        }

        print("Remaining workers continued training: PASS")

        # ---------------------------------------------
        # 6. Verify exact parameter correctness
        # ---------------------------------------------

        # Successful Pushes:
        #   A: iterations 1, 2, 3 = 3
        #   B: iteration 1       = 1
        #   C: iterations 2, 3   = 2
        #
        # Six Pushes with gradient 1.0, learning rate 0.1.
        values = worker_a.pull(
            [10]
        ).wait(timeout_ms=5000).values

        assert all(
            isclose(v, -0.6, abs_tol=1e-9)
            for v in values
        )

        print("All six updates applied exactly once: PASS")

        # The parameter server must retain clock history
        # for both active AND departed workers.
        clock = service.range_clocks[
            "p1"
        ].clock_for_key(10)

        assert clock == {
            1101: 3,
            1102: 1,
            1103: 3,
        }

        print("Historical vector clocks preserved: PASS")

        # Required-clock Pull includes a departed worker.
        required_values = worker_a.client.pull(
            [10],
            required_clock={
                1101: 3,
                1102: 1,
                1103: 3,
            },
        ).wait(timeout_ms=5000).values

        assert all(
            isclose(v, -0.6, abs_tol=1e-9)
            for v in required_values
        )

        print("Historical clock dependencies satisfied: PASS")

        # ---------------------------------------------
        # 7. Backwards compatibility
        # ---------------------------------------------

        # Older Part C requests omit worker_id.
        reply = stub.CanStart(
            manager_pb.CanStartRequest(iteration=1),
            timeout=5.0,
        )

        assert reply.allowed

        print("Legacy CanStart RPC preserved: PASS")

        print("E3.2 dynamic worker membership: PASS")

    finally:
        for worker in workers.values():
            worker.close()

        channel.close()
        grpc_manager.stop(0).wait()
        server.stop(0).wait()


if __name__ == "__main__":
    main()
