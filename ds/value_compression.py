
import struct
import zlib


def compress_doubles(values):
    """Losslessly compress float64 values."""
    values = list(values)

    # Explicit little-endian format for portability.
    raw = struct.pack(f"<{len(values)}d", *values)

    return zlib.compress(raw, level=3)


def decompress_doubles(data, count):
    """Decode exactly count float64 values."""

    if type(count) is not int or count < 0:
        raise ValueError("Invalid value count")

    expected_size = count * 8

    try:
        decoder = zlib.decompressobj()
        raw = decoder.decompress(data, expected_size + 1)

        if (
            len(raw) != expected_size
            or not decoder.eof
            or decoder.unused_data
            or decoder.unconsumed_tail
        ):
            raise ValueError("Invalid compressed value payload")

    except zlib.error as error:
        raise ValueError(
            "Invalid compressed value payload"
        ) from error

    return list(struct.unpack(f"<{count}d", raw))
