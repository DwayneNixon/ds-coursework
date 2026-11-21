"""Reusable local gRPC test-cluster fixtures and clock inspection helpers."""
from concurrent import futures

import grpc
import parameter_server_pb2_grpc as pb_grpc

from server import ParameterServer
from routing import KEY_SPACE_END


def start_server():
    # Create an independent gRPC server instance.
    grpc_server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4)
    )

    parameter_server_pb2_grpc.add_ParameterServerServicer_to_server(
        ParameterServer(value_width=2),
        grpc_server,
    )

    # Port 0 asks the OS to allocate a free local port.
    port = grpc_server.add_insecure_port("127.0.0.1:0")

    if port == 0:
        raise RuntimeError("Unable to bind gRPC server")

    grpc_server.start()
    return grpc_server, f"127.0.0.1:{port}"


def make_server():
    grpc_server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4)
    )

    service = ParameterServer(value_width=2)

    pb_grpc.add_ParameterServerServicer_to_server(
        service, grpc_server
    )

    port = grpc_server.add_insecure_port("127.0.0.1:0")
    assert port != 0

    return grpc_server, service, f"127.0.0.1:{port}"


def clock_counter(response, key, worker_id):
    """Read one worker's clock counter for a particular key."""

    matches = []

    for segment in response.clocks:
        end = (
            KEY_SPACE_END
            if segment.end_is_keyspace_end
            else segment.end
        )

        if segment.begin <= key < end:
            counter = next(
                (
                    entry.counter
                    for entry in segment.entries
                    if entry.worker_id == worker_id
                ),
                0,
            )

            matches.append(counter)

    assert len(matches) == 1, (
        f"Expected one clock segment covering key {key}"
    )

    return matches[0]


