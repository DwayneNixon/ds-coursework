
from concurrent import futures

import grpc

import parameter_server_pb2_grpc as pb_grpc

from server import ParameterServer
from parameter_client import ParameterClient


def main():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=8)
    )

    pb_grpc.add_ParameterServerServicer_to_server(
        ParameterServer(),
        server
    )

    port = server.add_insecure_port("127.0.0.1:0")
    server.start()

    address = f"127.0.0.1:{port}"

    client = ParameterClient(address, worker_id=7)
    other_client = ParameterClient(address, worker_id=8)

    try:
        # TEST 1: Sequential operations receive different IDs.
        first = client.push([7000], [10.0, 20.0], width=2)
        second = client.push([7000], [10.0, 20.0], width=2)

        assert first.task_id == (7, 1)
        assert second.task_id == (7, 2)

        assert first.wait(timeout_ms=3000).accepted
        assert second.wait(timeout_ms=3000).accepted

        values = list(
            client.pull([7000]).wait(timeout_ms=3000).values
        )

        assert values == [-2.0, -4.0]

        print("First ID:", first.task_id)
        print("Second ID:", second.task_id)
        print("Two distinct updates:", values)

        # TEST 2: Concurrent calls generate unique IDs.
        def submit_push(index):
            return client.push(
                [7100 + index],
                [10.0, 20.0],
                width=2
            )

        with futures.ThreadPoolExecutor(max_workers=6) as pool:
            tasks = list(pool.map(submit_push, range(12)))

        ids = [task.task_id for task in tasks]

        assert len(ids) == len(set(ids))
        assert {sequence for _, sequence in ids} == set(range(3, 15))

        for task in tasks:
            assert task.wait(timeout_ms=3000).accepted

        values = list(
            client.pull(range(7100, 7112))
            .wait(timeout_ms=3000).values
        )

        assert values == [-1.0, -2.0] * 12

        print("Concurrent IDs unique: True")
        print("Concurrent pushes applied: 12")

        # TEST 3: Another worker has its own sequence.
        other = other_client.push(
            [7000],
            [10.0, 20.0],
            width=2
        )

        assert other.task_id == (8, 1)
        assert other.wait(timeout_ms=3000).accepted

        final_values = list(
            client.pull([7000]).wait(timeout_ms=3000).values
        )

        assert final_values == [-3.0, -6.0]

        print("Different worker ID:", other.task_id)
        print("Final key 7000:", final_values)

        print("All Stage 5B checks passed!")

    finally:
        client.close()
        other_client.close()
        server.stop(grace=0)


if __name__ == "__main__":
    main()
