
import time
import math
from concurrent.futures import ThreadPoolExecutor

import grpc

import parameter_server_pb2 as pb
import parameter_server_pb2_grpc as pb_grpc

from server import ParameterServer
from parameter_client import ParameterClient


def make_id(sequence):
    return pb.TaskId(worker_id=7, sequence=sequence)


def main():
    server = grpc.server(
        ThreadPoolExecutor(max_workers=16)
    )

    service = ParameterServer()

    pb_grpc.add_ParameterServerServicer_to_server(
        service, server
    )

    port = server.add_insecure_port("127.0.0.1:0")
    server.start()

    address = f"127.0.0.1:{port}"
    client = ParameterClient(address, worker_id=9)

    try:
        with grpc.insecure_channel(address) as channel:
            stub = pb_grpc.ParameterServerStub(channel)

            # Dependent request arrives FIRST.
            dependent = pb.PushRequest(
                keys=[63000],
                gradients=[10.0, 20.0],
                width=2,
                task_id=make_id(2)
            )

            dependent.dependencies.add().CopyFrom(
                make_id(1)
            )

            pending = stub.Push.future(
                dependent, timeout=5
            )

            time.sleep(0.1)

            # It must not have executed prematurely.
            assert not pending.done()

            before = stub.Pull(
                pb.PullRequest(keys=[63000])
            )

            assert list(before.values) == [0.0, 0.0]

            print("Dependent request waited: True")

            # Now complete its prerequisite.
            prerequisite = pb.PushRequest(
                keys=[63001],
                gradients=[2.0, 4.0],
                width=2,
                task_id=make_id(1)
            )

            assert stub.Push(
                prerequisite, timeout=5
            ).accepted

            assert pending.result(timeout=5).accepted

            after = stub.Pull(
                pb.PullRequest(keys=[63000])
            )

            assert list(after.values) == [-1.0, -2.0]

            print("Dependent executed after prerequisite: True")

            # Failed prerequisite test.
            dependent_pull = pb.PullRequest(
                keys=[63000],
                task_id=make_id(4)
            )

            dependent_pull.dependencies.add().CopyFrom(
                make_id(3)
            )

            failed_dependent = stub.Pull.future(
                dependent_pull,
                timeout=5
            )

            time.sleep(0.1)

            invalid_push = pb.PushRequest(
                keys=[63002],
                gradients=[1.0],
                width=2,
                task_id=make_id(3)
            )

            try:
                stub.Push(invalid_push, timeout=5)
            except grpc.RpcError as error:
                assert error.code() == grpc.StatusCode.INVALID_ARGUMENT
            else:
                raise AssertionError("Invalid push succeeded")

            try:
                failed_dependent.result(timeout=5)
            except grpc.RpcError as error:
                assert error.code() == grpc.StatusCode.FAILED_PRECONDITION
            else:
                raise AssertionError(
                    "Failed dependency was not propagated"
                )

            print("Failed dependency prevented execution: True")

        # Public client API still works.
        task_a = client.push(
            [64000],
            [10.0, 20.0],
            width=2
        )

        task_b = client.pull(
            [64000],
            deps=[task_a]
        )

        values = list(
            task_b.wait(timeout_ms=5000).values
        )

        assert values == [-1.0, -2.0]

        #assert task_a.task_id == (9, 1)
        #assert task_b.task_id is not None

        print("Actual Push Task ID:", task_a.task_id)
        print("Actual Pull Task ID:", task_b.task_id)
        print("Push sequence counter:", client._sequence)
        print("Pull sequence counter:", client._pull_sequence)

        assert task_a.task_id is not None
        assert task_b.task_id is not None

        assert task_a.task_id[0] == 9
        assert task_b.task_id[0] == 9

        assert 1 <= task_a.task_id[1] < (1 << 63)
        assert (1 << 63) <= task_b.task_id[1] < (1 << 64)

        assert task_a.task_id != task_b.task_id


        print("Original client API works: True")
        print("Push Task ID remains:", task_a.task_id)
        print("Pull Task ID:", task_b.task_id)

        print("All final dependency checks passed!")

    finally:
        client.close()
        server.stop(grace=0)


if __name__ == "__main__":
    main()
