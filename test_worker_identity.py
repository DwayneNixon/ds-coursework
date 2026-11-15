
from concurrent.futures import ThreadPoolExecutor

import grpc

import parameter_server_pb2_grpc as pb_grpc

from server import ParameterServer
from parameter_client import ParameterClient


def main():
    service = ParameterServer()

    server = grpc.server(
        ThreadPoolExecutor(max_workers=4)
    )

    pb_grpc.add_ParameterServerServicer_to_server(
        service,
        server
    )

    port = server.add_insecure_port("127.0.0.1:0")
    server.start()

    address = f"127.0.0.1:{port}"

    # Two independently created clients share worker ID 7.
    worker_a = ParameterClient(address, worker_id=7)
    worker_b = ParameterClient(address, worker_id=7)

    # A different worker identity.
    worker_c = ParameterClient(address, worker_id=8)

    # Another new instance reusing worker ID 7.
    worker_d = ParameterClient(address, worker_id=7)

    def read_weights():
        response = worker_a.pull([62000]).wait(
            timeout_ms=3000
        )
        return list(response.values)

    try:
        # First logical update.
        task_a = worker_a.push(
            [62000],
            [10.0, 20.0],
            width=2
        )

        assert task_a.wait(timeout_ms=3000).accepted

        first = read_weights()

        assert first == [-1.0, -2.0]

        print("Worker A Task ID:", task_a.task_id)
        print("After Worker A:", first)

        # A second client submits an identical operation.
        task_b = worker_b.push(
            [62000],
            [10.0, 20.0],
            width=2
        )

        assert task_b.wait(timeout_ms=3000).accepted

        second = read_weights()

        print("Worker B Task ID:", task_b.task_id)
        print("After Worker B:", second)

        # The server recognises this as a duplicate.
        assert task_a.task_id == task_b.task_id
        assert second == first

        print("Identical worker IDs collide: True")
        print("Second logical update suppressed: True")

        # Reusing the same ID with different contents.
        task_d = worker_d.push(
            [62000],
            [30.0, 40.0],
            width=2
        )

        try:
            task_d.wait(timeout_ms=3000)

        except grpc.RpcError as error:
            assert error.code() == grpc.StatusCode.INVALID_ARGUMENT
            print("Conflicting collision:", error.code().name)

        else:
            raise AssertionError(
                "Conflicting Task ID was accepted"
            )

        # Different worker IDs should not collide.
        task_c = worker_c.push(
            [62000],
            [10.0, 20.0],
            width=2
        )

        assert task_c.wait(timeout_ms=3000).accepted

        final = read_weights()

        assert final == [-2.0, -4.0]

        print("Worker C Task ID:", task_c.task_id)
        print("After distinct worker:", final)

        print("Stage 9B.1 diagnostic passed!")

    finally:
        worker_a.close()
        worker_b.close()
        worker_c.close()
        worker_d.close()
        server.stop(grace=0)


if __name__ == "__main__":
    main()
