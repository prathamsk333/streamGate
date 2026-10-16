import pytest

from streamgate.codec import DecodeError, JsonCodec

codec = JsonCodec()


def test_round_trip() -> None:
    data = {"name": "Dosa café", "n": [1, 2.5, None]}
    assert codec.decode(codec.encode(data)) == data
    assert codec.encode({"a": 1}) == b'{"a":1}'


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        (b"not json", "invalid JSON"),
        (b"[1, 2]", "expected a JSON object, got list"),
        (b'"text"', "expected a JSON object, got str"),
        (b'{"x": NaN}', "invalid JSON: NaN is not valid JSON"),
        (b"\xff\xfe\xfa", "not valid UTF-8"),
        (b"", "invalid JSON"),
    ],
)
def test_decode_errors(raw: bytes, reason: str) -> None:
    with pytest.raises(DecodeError, match=reason):
        codec.decode(raw)
