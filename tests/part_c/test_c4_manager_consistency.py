
import grpc
from concurrent import futures

import manager_pb2 as pb
import manager_pb2_grpc as pb_grpc

from routing import Partition, KEY_SPACE_END
from server_manager import ServerManager
from manager_rpc import ManagerService


def start_manager(mode, tau=0):
    manager = ServerManager([
        Partition(
            "p1", 0, KEY_SPACE_END, "test-server"
        )
    ])

    manager.configure_consistency(
        workers=[1, 2],
        mode=mode,
        tau=tau,
    )

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4)
    )

    pb_grpc.add_OwnershipManagerServicer_to_server(
        ManagerService(manager),
        server,
    )

    port = server.add_insecure_port("127.0.0.1:0")
    assert port != 0

    server.start()
    return server, f"127.0.0.1:{port}"


def can_start(stub, iteration):
    return stub.CanStart(
        pb.CanStartRequest(iteration=iteration),
        timeout=5,
    )


def complete(stub, worker_id, iteration):
    return stub.CompleteIteration(
        pb.CompleteIterationRequest(
            worker_id=worker_id,
            iteration=iteration,
        ),
        timeout=5,
    )


def test_sequential():
    server, address = start_manager("sequential")

    try:
        with (
            grpc.insecure_channel(address) as channel1,
            grpc.insecure_channel(address) as channel2
        ):
            worker1 = pb_grpc.OwnershipManagerStub(channel1)
            worker2 = pb_grpc.OwnershipManagerStub(channel2)

            assert can_start(worker1, 1).allowed
            assert not can_start(worker1, 2).allowed

            complete(worker1, 1, 1)

            # Worker 1 cannot advance until Worker 2 finishes.
            assert not can_start(worker1, 2).allowed

            complete(worker2, 2, 1)

            response = can_start(worker1, 2)
            assert response.allowed
            assert response.required_iteration == 1

            print("Sequential multiworker barrier: PASS")

            # A worker cannot skip completed iterations.
            try:
                complete(worker1, 1, 3)
            except grpc.RpcError as error:
                assert error.code() == (
                    grpc.StatusCode.INVALID_ARGUMENT
                )
            else:
                raise AssertionError(
                    "Skipped iteration was accepted"
                )

            print("Invalid completion rejected: PASS")
            print("Separate gRPC worker channels: PASS")

    finally:
        server.stop(0).wait()


def test_bounded():
    server, address = start_manager("bounded", tau=2)

    try:
        with grpc.insecure_channel(address) as channel:
            worker = pb_grpc.OwnershipManagerStub(channel)

            assert can_start(worker, 1).allowed
            assert can_start(worker, 2).allowed
            assert can_start(worker, 3).allowed

            # Iteration 4 requires both workers to complete 1.
            assert not can_start(worker, 4).allowed

            complete(worker, 1, 1)
            assert not can_start(worker, 4).allowed

            complete(worker, 2, 1)
            assert can_start(worker, 4).allowed

            print("Bounded-delay tau=2 coordination: PASS")

    finally:
        server.stop(0).wait()


def test_eventual():
    server, address = start_manager("eventual")

    try:
        with grpc.insecure_channel(address) as channel:
            worker = pb_grpc.OwnershipManagerStub(channel)

            response = can_start(worker, 100)

            assert response.allowed
            assert response.required_iteration == 0

            print("Eventual consistency coordination: PASS")

    finally:
        server.stop(0).wait()


def main():
    test_sequential()
    test_bounded()
    test_eventual()

    print("C4.2A gRPC consistency coordinator: PASS")


if __name__ == "__main__":
    main()
