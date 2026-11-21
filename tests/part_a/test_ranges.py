
import math
import grpc

import parameter_server_pb2 as pb
import parameter_server_pb2_grpc as pb_grpc


def main():
    with grpc.insecure_channel("127.0.0.1:50051") as channel:
        stub = pb_grpc.ParameterServerStub(channel)

        keys = [10, 11, 12]
        gradients = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]

        sparse = pb.PullRequest(keys=keys)
        dense = pb.PullRequest(
            key_range=pb.KeyRange(begin=10, end=13)
        )

        # Test 1: Both access modes start with equal values.
        before_sparse = list(stub.Pull(sparse).values)
        before_dense = list(stub.Pull(dense).values)

        print("Initial sparse:", before_sparse)
        print("Initial dense: ", before_dense)

        assert before_sparse == before_dense
        assert before_sparse == [0.0] * 6

        # Test 2: Push using a sparse key list.
        stub.Push(
            pb.PushRequest(
                keys=keys,
                gradients=gradients,
                width=2
            )
        )

        after_sparse = list(stub.Pull(sparse).values)
        after_dense = list(stub.Pull(dense).values)

        print("After sparse push:", after_sparse)

        assert after_sparse == after_dense
        assert after_sparse == [
            -1.0, -2.0, -3.0,
            -4.0, -5.0, -6.0
        ]

        # Test 3: Push the same gradients using a range.
        stub.Push(
            pb.PushRequest(
                key_range=pb.KeyRange(begin=10, end=13),
                gradients=gradients,
                width=2
            )
        )

        final_values = list(stub.Pull(dense).values)

        print("After dense push: ", final_values)

        expected = [-2.0, -4.0, -6.0, -8.0, -10.0, -12.0]

        assert all(
            math.isclose(actual, target)
            for actual, target in zip(final_values, expected)
        )
        assert len(final_values) == len(expected)

        # Test 4: Reject ambiguous requests.
        try:
            stub.Pull(
                pb.PullRequest(
                    keys=[10],
                    key_range=pb.KeyRange(begin=10, end=13)
                )
            )
        except grpc.RpcError as error:
            print("Mixed target error:", error.code().name)
            assert error.code() == grpc.StatusCode.INVALID_ARGUMENT
        else:
            raise AssertionError("Mixed target was accepted")

        # Test 5: Reject a reversed range.
        try:
            stub.Pull(
                pb.PullRequest(
                    key_range=pb.KeyRange(begin=13, end=10)
                )
            )
        except grpc.RpcError as error:
            print("Invalid range error:", error.code().name)
            assert error.code() == grpc.StatusCode.INVALID_ARGUMENT
        else:
            raise AssertionError("Invalid range was accepted")

        print("All Stage 2B checks passed!")


if __name__ == "__main__":
    main()
