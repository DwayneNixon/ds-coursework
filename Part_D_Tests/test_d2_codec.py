
import struct
import parameter_server_pb2 as pb

from value_compression import (
    compress_doubles,
    decompress_doubles,
)


def main():
    # The protocol must expose our new fields.
    assert "compressed_gradients" in (
        pb.PushRequest.DESCRIPTOR.fields_by_name
    )
    assert "accept_compressed_values" in (
        pb.PullRequest.DESCRIPTOR.fields_by_name
    )
    assert "compressed_values" in (
        pb.PullResponse.DESCRIPTOR.fields_by_name
    )

    print("D2 Protobuf fields: PASS")

    # Check lossless round-trip, including negative zero.
    values = [0.0, -0.0, 1.5, -2.75, 1e20]

    compressed = compress_doubles(values)
    restored = decompress_doubles(compressed, len(values))

    assert struct.pack("<5d", *restored) == (
        struct.pack("<5d", *values)
    )

    print("Lossless float64 round-trip: PASS")

    # Repeated values should compress effectively.
    repeated = [1.0, 2.0] * 64

    compressed = compress_doubles(repeated)

    assert len(compressed) < len(repeated) * 8
    assert decompress_doubles(
        compressed, len(repeated)
    ) == repeated

    print("Repeated-value compression: PASS")
    print(
        f"Raw: {len(repeated) * 8} bytes | "
        f"Compressed: {len(compressed)} bytes"
    )

    # Incorrect value counts must be rejected.
    try:
        decompress_doubles(compressed, len(repeated) - 1)
        raise AssertionError("Expected invalid length")

    except ValueError:
        pass

    print("Invalid payload length rejected: PASS")
    print("D2.1 codec: PASS")


if __name__ == "__main__":
    main()
