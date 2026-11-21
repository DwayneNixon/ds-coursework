
import math
from itertools import count
from concurrent.futures import ThreadPoolExecutor

import grpc

import parameter_server_pb2 as pb
import parameter_server_pb2_grpc as pb_grpc

from server import ParameterServer


UINT64_MAX = (1 << 64) - 1


def main():
    service = ParameterServer()

    server = grpc.server(
        ThreadPoolExecutor(max_workers=8)
    )

    pb_grpc.add_ParameterServerServicer_to_server(
        service, server
    )

    port = server.add_insecure_port("127.0.0.1:0")
    server.start()

    failures = []
    sequence = count(1)

    def check(name, passed, detail=""):
        result = "PASS" if passed else "FAIL"
        message = f"{result}: {name}"

        if detail:
            message += f" ({detail})"

        print(message)

        if not passed:
            failures.append(name)

    try:
        with grpc.insecure_channel(
            f"127.0.0.1:{port}"
        ) as channel:

            stub = pb_grpc.ParameterServerStub(channel)

            def call(rpc, request):
                try:
                    response = rpc(request, timeout=5)
                    return "OK", response

                except grpc.RpcError as error:
                    return error.code().name, None

            def push(
                keys=None,
                gradients=None,
                width=2,
                key_range=None
            ):
                request = pb.PushRequest(
                    gradients=gradients or [],
                    width=width,
                    task_id=pb.TaskId(
                        worker_id=91,
                        sequence=next(sequence)
                    )
                )

                if keys is not None:
                    request.keys.extend(keys)

                if key_range is not None:
                    request.key_range.CopyFrom(key_range)

                status, response = call(
                    stub.Push, request
                )

                if status == "OK" and not response.accepted:
                    return "NOT_ACCEPTED"

                return status

            def pull(keys=None, key_range=None):
                request = pb.PullRequest()

                if keys is not None:
                    request.keys.extend(keys)

                if key_range is not None:
                    request.key_range.CopyFrom(key_range)

                status, response = call(
                    stub.Pull, request
                )

                values = (
                    list(response.values)
                    if response is not None
                    else None
                )

                return status, values

            def expect_invalid(name, **kwargs):
                before = pull(keys=[20010, 20011])

                status = push(**kwargs)

                after = pull(keys=[20010, 20011])

                check(
                    name,
                    status == "INVALID_ARGUMENT"
                    and before == after,
                    f"status={status}, unchanged={before == after}"
                )

            # ------------------------------------
            # TEST 1: Sparse and dense equivalence
            # ------------------------------------

            status = push(
                keys=[20010, 20011],
                gradients=[10.0, 20.0, 30.0, 40.0],
                width=2
            )

            assert status == "OK", (
                f"Initial push failed: {status}"
            )

            sparse_status, sparse_values = pull(
                keys=[20010, 20011]
            )

            dense_status, dense_values = pull(
                key_range=pb.KeyRange(
                    begin=20010,
                    end=20012
                )
            )

            check(
                "Sparse/dense equivalence",
                sparse_status == "OK"
                and dense_status == "OK"
                and sparse_values == dense_values
                and len(sparse_values) == 4,
                f"values={sparse_values}"
            )

            # ------------------------------------
            # TEST 2: Wrong gradient count
            # ------------------------------------

            expect_invalid(
                "Gradient length mismatch",
                keys=[20010],
                gradients=[1.0],
                width=2
            )

            # ------------------------------------
            # TEST 3: Zero vector width
            # ------------------------------------

            expect_invalid(
                "Zero width",
                keys=[20010],
                gradients=[1.0, 2.0],
                width=0
            )

            # ------------------------------------
            # TEST 4: Duplicate keys
            # ------------------------------------

            expect_invalid(
                "Duplicate keys",
                keys=[20010, 20010],
                gradients=[1.0, 2.0, 3.0, 4.0],
                width=2
            )

            # ------------------------------------
            # TEST 5: Reversed dense range
            # ------------------------------------

            expect_invalid(
                "Reversed range",
                key_range=pb.KeyRange(
                    begin=20012,
                    end=20010
                ),
                gradients=[],
                width=2
            )

            # ------------------------------------
            # TEST 6: Mixed target formats
            # ------------------------------------

            expect_invalid(
                "Keys and range together",
                keys=[20010],
                key_range=pb.KeyRange(
                    begin=20010,
                    end=20011
                ),
                gradients=[1.0, 2.0],
                width=2
            )

            # ------------------------------------
            # TEST 7: uint64 boundary values
            # ------------------------------------

            status, values = pull(
                keys=[0, UINT64_MAX]
            )

            check(
                "uint64 boundary keys",
                status == "OK"
                and values == [0.0, 0.0, 0.0, 0.0],
                f"status={status}, values={values}"
            )

            # ------------------------------------
            # TEST 8: Invalid unsigned integers
            # ------------------------------------

            for key in [-1, UINT64_MAX + 1]:
                try:
                    pb.PullRequest(keys=[key])
                    rejected = False

                except (ValueError, OverflowError, TypeError):
                    rejected = True

                check(
                    f"Protobuf rejects key {key}",
                    rejected
                )

            # ------------------------------------
            # TEST 9: Empty range
            # ------------------------------------

            before = pull(keys=[20010, 20011])

            empty_status = push(
                key_range=pb.KeyRange(
                    begin=20010,
                    end=20010
                ),
                gradients=[],
                width=2
            )

            after = pull(keys=[20010, 20011])

            check(
                "Empty push does not alter parameters",
                before == after,
                f"status={empty_status}"
            )

            empty_pull_status, empty_values = pull(
                key_range=pb.KeyRange(
                    begin=20010,
                    end=20010
                )
            )

            print(
                "DIAGNOSTIC: Empty range push:",
                empty_status
            )
            print(
                "DIAGNOSTIC: Empty range pull:",
                empty_pull_status,
                empty_values
            )

            # ------------------------------------
            # TEST 10: Different vector width
            # ------------------------------------

            width3_status = push(
                keys=[20100],
                gradients=[1.0, 2.0, 3.0],
                width=3
            )

            print(
                "DIAGNOSTIC: Width 3 push:",
                width3_status
            )

            
            # ------------------------------------
            # TEST 11: Non-finite gradients
            # ------------------------------------

            non_finite_cases = [
                ("NaN", math.nan),
                ("Positive infinity", math.inf),
                ("Negative infinity", -math.inf)
            ]

            for index, (name, value) in enumerate(non_finite_cases):
                key = 20200 + index

                before = pull(keys=[key])

                status = push(
                    keys=[key],
                    gradients=[value, 1.0],
                    width=2
                )

                after = pull(keys=[key])

                check(
                    f"{name} rejected",
                    status == "INVALID_ARGUMENT"
                    and before == after,
                    f"status={status}, unchanged={before == after}"
                )

            # ------------------------------------
            # Summary
            # ------------------------------------

            print()
            print("Core checks failed:", len(failures))

            if failures:
                print("Failed checks:")
                for name in failures:
                    print(" -", name)

                raise AssertionError(
                    "Stage 8A found validation failures"
                )

            print("All Stage 8A core checks passed!")

    finally:
        server.stop(grace=0)


if __name__ == "__main__":
    main()
