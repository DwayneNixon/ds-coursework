
import math
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

    client_a = ParameterClient(address, worker_id=101)
    client_b = ParameterClient(address, worker_id=102)

    def read_weights():
        response = client_a.pull([60000]).wait(
            timeout_ms=3000
        )
        return list(response.values)

    try:
        # TEST 1: SGD must still be the default.
        assert service.active_update_rule == "sgd"
        assert read_weights() == [0.0, 0.0]

        print("Default rule:", service.active_update_rule)

        # TEST 2: Register through the client.
        registered = client_a.register_update_rule(
            "half_add",
            gradient_scale=0.5
        )

        assert registered
        assert "half_add" in service.update_rules

        # Registration alone must not change the active rule.
        assert service.active_update_rule == "sgd"

        print("Remote registration succeeded:", registered)
        print("Registration preserves active rule: True")

        # TEST 3: Activate the registered rule.
        selected = client_a.set_update_rule("half_add")

        assert selected == "half_add"

        task = client_a.push(
            [60000],
            [10.0, 20.0],
            width=2
        )

        assert task.wait(timeout_ms=3000).accepted

        assert read_weights() == [5.0, 10.0]

        print("After half-add:", read_weights())

        # TEST 4: A second worker uses the same rule.
        task = client_b.push(
            [60000],
            [2.0, 4.0],
            width=2
        )

        assert task.wait(timeout_ms=3000).accepted

        assert read_weights() == [6.0, 12.0]

        print("After second worker:", read_weights())

        # TEST 5: Duplicate rule names are rejected.
        try:
            client_a.register_update_rule(
                "half_add",
                gradient_scale=1.0
            )

        except grpc.RpcError as error:
            assert error.code() == grpc.StatusCode.INVALID_ARGUMENT
            print("Duplicate registration:", error.code().name)

        else:
            raise AssertionError("Duplicate rule was accepted")

        # TEST 6: Invalid rule configurations are rejected.
        for name, scale in [
            ("infinite_rule", math.inf),
            ("nan_rule", math.nan),
            ("   ", 1.0),
        ]:
            try:
                client_a.register_update_rule(name, scale)

            except grpc.RpcError as error:
                assert error.code() == grpc.StatusCode.INVALID_ARGUMENT

            else:
                raise AssertionError(
                    f"Invalid rule {name!r} was accepted"
                )

        print("Invalid registrations rejected: True")

        # TEST 7: Restore the default SGD rule.
        assert client_b.set_update_rule("sgd") == "sgd"

        task = client_a.push(
            [60000],
            [10.0, 20.0],
            width=2
        )

        assert task.wait(timeout_ms=3000).accepted

        assert read_weights() == [5.0, 10.0]

        print("After restoring SGD:", read_weights())

        print("All Stage 9A checks passed!")

    finally:
        client_a.close()
        client_b.close()
        server.stop(grace=0)


if __name__ == "__main__":
    main()
