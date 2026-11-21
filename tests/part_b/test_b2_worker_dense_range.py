"""Ensure B2's worker preserves native dense ranges during gRPC training."""

import math
from concurrent.futures import ThreadPoolExecutor

import grpc
import parameter_server_pb2_grpc as pb_grpc

from server import ParameterServer
from parameter_client import ParameterClient
from harness.worker import run_worker


class FixedDenseWorkload:
    def sample_keys(self):
        return range(10, 14)

    def sample_compute_ms(self):
        return 0


class RecordingClient:
    def __init__(self, client):
        self.client = client
        self.targets_seen = []

    def pull(self, target, **kwargs):
        self.targets_seen.append(("pull", target))
        return self.client.pull(target, **kwargs)

    def push(self, target, gradients, **kwargs):
        self.targets_seen.append(("push", target))
        return self.client.push(target, gradients, **kwargs)


def main():
    service = ParameterServer()
    rpc_server = grpc.server(ThreadPoolExecutor(max_workers=8))
    pb_grpc.add_ParameterServerServicer_to_server(service, rpc_server)
    port = rpc_server.add_insecure_port("127.0.0.1:0")
    assert port > 0
    rpc_server.start()
    base = ParameterClient(f"127.0.0.1:{port}", worker_id=401)
    recording = RecordingClient(base)

    try:
        targets = {key: [1.0, -1.0] for key in range(10, 14)}
        run_worker(
            client=recording,
            worker_id=401,
            iterations=2,
            num_keys=100,
            keys_per_iter=4,
            width=2,
            targets=targets,
            seed=42,
            workload=FixedDenseWorkload(),
        )
        assert [op for op, _ in recording.targets_seen] == [
            "pull", "push", "pull", "push"
        ]
        assert all(
            isinstance(target, range) and target == range(10, 14)
            for _, target in recording.targets_seen
        ), recording.targets_seen

        values = list(base.pull(range(10, 14)).wait(timeout_ms=5000).values)
        expected = [0.19, -0.19] * 4
        assert len(values) == len(expected)
        assert all(math.isclose(a, b, abs_tol=1e-12)
                   for a, b in zip(values, expected)), (values, expected)
        print("B2 worker native dense-range integration: PASS")
    finally:
        base.close()
        rpc_server.stop(0).wait()


if __name__ == "__main__":
    main()
