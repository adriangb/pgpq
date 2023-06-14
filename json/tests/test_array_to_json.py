import json
from typing import Any, Dict, List
import pyarrow as pa
import pytest

from arrow_json import array_to_utf8_json_array


int_array = pa.array([1, 2, 3, 4])
string_array = pa.array(["A", "B", "C", "D"])
struct_array = pa.StructArray.from_arrays(
    [int_array, string_array], names=["int", "str"]
)
list_array = pa.ListArray.from_arrays(offsets=[0, 2, 4, 6, 8], values=[1, 2, 3, 4] * 2)


@pytest.mark.parametrize(
    "array, expected",
    [
        (int_array, [1, 2, 3, 4]),
        (string_array, ["A", "B", "C", "D"]),
        (
            struct_array,
            [
                {"int": 1, "str": "A"},
                {"int": 2, "str": "B"},
                {"int": 3, "str": "C"},
                {"int": 4, "str": "D"},
            ],
        ),
        (list_array, [[1, 2], [3, 4], [1, 2], [3, 4]]),
    ],
    ids=["int", "string", "struct", "list"],
)
def test_to_array(array: pa.Array, expected: List[Any]) -> None:
    expected = [json.dumps(v, separators=(",", ":")) for v in expected]
    actual = array_to_utf8_json_array(array).to_pylist()
    assert actual == expected


@pytest.mark.parametrize(
    "kwargs,expected",
    [
        ({"large": True}, pa.large_string()),
        ({"large": False}, pa.string()),
        ({}, pa.large_string()),
    ],
)
def test_large(kwargs: Dict[str, Any], expected: pa.DataType) -> None:
    array = pa.array([1, 2, 3])
    out = array_to_utf8_json_array(array, **kwargs)
    assert out.type == expected
