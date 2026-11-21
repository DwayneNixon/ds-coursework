"""A1: reject oversized sparse/dense requests before accessing parameters."""

from concurrent.futures import ThreadPoolExecutor

import grpc

import parameter_server_pb2 as pb
import parameter_server_pb2_grpc as pb_grpc
from server import ParameterServer


def expect_exhausted(rpc, request):
    try:
        rpc(request, timeout=3)
    except grpc.RpcError as error:
        assert error.code() == grpc.StatusCode.RESOURCE_EXHAUSTED, error
    else:
        raise AssertionError("Oversized parameter request was accepted")


def main():
    for wrong in (-1, 0, 1, 2.5, True):
        try:
            ParameterServer(value_width=2, max_values_per_request=wrong)
        except ValueError:
            pass
        else:
            raise AssertionError(f"Invalid request budget accepted: {wrong!r}")

    service = ParameterServer(value_width=2, max_values_per_request=8)
    server = grpc.server(ThreadPoolExecutor(max_workers=4))
    pb_grpc.add_ParameterServerServicer_to_server(service, server)
    port = server.add_insecure_port("127.0.0.1:0")
    assert port > 0
    server.start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            stub = pb_grpc.ParameterServerStub(channel)

            # Four width-2 parameters fit the configured eight-value budget.
            valid = pb.KeyRange(begin=100, end=104)
            assert len(stub.Pull(pb.PullRequest(key_range=valid), timeout=3).values) == 8
            assert stub.Push(
                pb.PushRequest(key_range=valid, gradients=[1.0] * 8, width=2),
                timeout=3,
            ).accepted
            before = list(stub.Pull(pb.PullRequest(key_range=valid), timeout=3).values)
            assert before == [-0.1] * 8

            # Dense and sparse cases must be rejected for both operations.
            too_large = pb.KeyRange(begin=100, end=105)
            expect_exhausted(stub.Pull, pb.PullRequest(key_range=too_large))
            expect_exhausted(stub.Push, pb.PushRequest(
                key_range=too_large, gradients=[1.0] * 10, width=2
            ))
            sparse = [100, 101, 102, 103, 104]
            expect_exhausted(stub.Pull, pb.PullRequest(keys=sparse))
            expect_exhausted(stub.Push, pb.PushRequest(
                keys=sparse, gradients=[1.0] * 10, width=2
            ))

            # A nearly full uint64 interval must not overflow len(range).
            huge = pb.KeyRange(begin=0, end=(1 << 64) - 1)
            expect_exhausted(stub.Pull, pb.PullRequest(key_range=huge))
            expect_exhausted(stub.Push, pb.PushRequest(key_range=huge, width=2))

            # Rejected requests must not change already committed values.
            after = list(stub.Pull(pb.PullRequest(key_range=valid), timeout=3).values)
            assert after == before
            print("A1 resource-bounded request validation: PASS")
    finally:
        server.stop(0).wait()


if __name__ == "__main__":
    main()
