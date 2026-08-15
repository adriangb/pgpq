from dataclasses import dataclass
from decimal import Decimal
from math import floor
from pathlib import Path
from typing import Any

import pyarrow as pa

DIR = (Path(__file__).parent / "testdata").resolve()
DIR.mkdir(exist_ok=True)


@dataclass
class Col:
    name: str
    type: pa.DataType
    data: list[Any]
    nullable: bool = False


timestamp_s = 167_614_2874
timestamp_ms = floor(timestamp_s * 1e3)
timestamp_us = floor(timestamp_s * 1e6)

time_s = 24 * 60 * 60 - 1
time_ms = floor(time_s * 1e3)
time_us = floor(time_s * 1e6)

date32 = 2**16 - 1

duration_s = 60
duration_ms = floor(duration_s * 1e3)
duration_us = floor(duration_s * 1e6)

string = "some data! "
large_string = "some large string"
binary = string.encode()
large_binary = large_string.encode()
fixed_size_binary_width = 3

primitive_cols: list[tuple[pa.field, list[Any]]] = [
    (pa.field("bool", pa.bool_()), [True, False]),
    (pa.field("uint8", pa.uint8()), [0, 1, 2]),
    (pa.field("uint16", pa.uint16()), [0, 1, 2]),
    (pa.field("uint32", pa.uint32()), [0, 1, 2]),
    (pa.field("uint64", pa.uint64()), [0, 1, 18446744073709551615]),
    (pa.field("int8", pa.int8()), [-1, 0, 1]),
    (pa.field("int16", pa.int16()), [-1, 0, 1]),
    (pa.field("int32", pa.int32()), [-1, 0, 1]),
    (pa.field("int64", pa.int64()), [-1, 0, 1]),
    (pa.field("float16", pa.float16()), [-1, 0, 1, float("inf")]),
    (pa.field("float32", pa.float32()), [-1, 0, 1, float("inf")]),
    (pa.field("float64", pa.float64()), [-1, 0, 1, float("inf")]),
    (
        pa.field("decimal32", pa.decimal32(9, 6)),
        [
            Decimal("0"),
            Decimal("0.000"),
            Decimal("0.001"),
            Decimal("123"),
            Decimal("123.45"),
            Decimal("-123.45"),
            Decimal("123.4567"),
            Decimal("123.45678"),
        ],
    ),
    (
        pa.field("decimal64", pa.decimal64(9, 6)),
        [
            Decimal("0"),
            Decimal("0.000"),
            Decimal("0.001"),
            Decimal("123"),
            Decimal("123.45"),
            Decimal("-123.45"),
            Decimal("123.4567"),
            Decimal("123.45678"),
        ],
    ),
    (
        pa.field("decimal128", pa.decimal128(9, 6)),
        [
            Decimal("0"),
            Decimal("0.000"),
            Decimal("0.001"),
            Decimal("123"),
            Decimal("123.45"),
            Decimal("-123.45"),
            Decimal("123.4567"),
            Decimal("123.45678"),
        ],
    ),
    (pa.field("timestamp_us_notz", pa.timestamp("us", None)), [0, 1, timestamp_us]),
    (pa.field("timestamp_ms_notz", pa.timestamp("ms", None)), [0, 1, timestamp_ms]),
    (pa.field("timestamp_s_notz", pa.timestamp("s", None)), [0, 1, timestamp_s]),
    (
        pa.field("timestamp_us_tz", pa.timestamp("us", "America/New_York")),
        [0, 1, timestamp_us],
    ),
    (
        pa.field("timestamp_ms_tz", pa.timestamp("ms", "America/New_York")),
        [0, 1, timestamp_ms],
    ),
    (
        pa.field("timestamp_s_tz", pa.timestamp("s", "America/New_York")),
        [0, 1, timestamp_s],
    ),
    (pa.field("time_s", pa.time32("s")), [0, 1, time_s]),
    (pa.field("time_ms", pa.time32("ms")), [0, 1, time_ms]),
    (pa.field("time_us", pa.time64("us")), [0, 1, time_us]),
    (pa.field("date32", pa.date32()), [0, -date32, date32]),
    (pa.field("duration_us", pa.duration("us")), [0, 1, duration_us]),
    (pa.field("duration_ms", pa.duration("ms")), [0, 1, duration_ms]),
    (pa.field("duration_s", pa.duration("s")), [0, 1, duration_s]),
    (pa.field("binary", pa.binary()), [b"", binary]),
    (pa.field("large_binary", pa.large_binary()), [b"", binary, large_binary]),
    (
        pa.field("fixed_size_binary", pa.binary(fixed_size_binary_width)),
        [b"\x00\x00\x00", b"abc", b"\xff\xfe\xfd"],
    ),
    (pa.field("string", pa.string()), ["", string]),
    (pa.field("large_string", pa.large_string()), ["", string, large_string]),
    (pa.field("string_view", pa.string_view()), ["", string, large_string]),
]

longest = max(len(c[1]) for c in primitive_cols)

# add nullable columns
nullable_primitives = [
    (f.with_name(f"{f.name}_nullable").with_nullable(True), [*data, None])
    for f, data in primitive_cols
]

list_cols = [
    (pa.field(f"list_{f.name}", pa.list_(f), nullable=False), [data])
    for f, data in [*primitive_cols, *nullable_primitives]
]

nullable_list_cols = [
    (pa.field(f"list_nullable_{f.name}", pa.list_(f), nullable=True), [data, None])
    for f, data in [*primitive_cols, *nullable_primitives]
]


# LargeList differs from List only in the width of the offsets, which the encoder
# never puts on the wire (a Postgres array carries element counts, not offsets), so a
# handful of element types is enough to pin the bytes down. The `large_list_` prefix
# keeps these from colliding with the `list_` names above.
LARGE_LIST_ELEMENTS = {"int32", "string", "int32_nullable", "string_nullable"}

large_list_cols = [
    (pa.field(f"large_list_{f.name}", pa.large_list(f), nullable=False), [data])
    for f, data in [*primitive_cols, *nullable_primitives]
    if f.name in LARGE_LIST_ELEMENTS
]

large_nullable_list_cols = [
    (
        pa.field(f"large_list_nullable_{f.name}", pa.large_list(f), nullable=True),
        [data, None],
    )
    for f, data in [*primitive_cols, *nullable_primitives]
    if f.name in LARGE_LIST_ELEMENTS
]

# A `FixedSizeList` shares its whole element encoding path with `List`, which is
# already swept over every primitive above; what is new is the fixed stride (there is
# no offsets buffer) and the list level validity. A representative slice of element
# types is therefore enough here, and keeps the number of committed snapshots down.
fixed_size_list_element_types = {
    "bool",
    "int32",
    "int64",
    "float64",
    "decimal128",
    "timestamp_us_notz",
    "string",
    "binary",
    "fixed_size_binary",
}

fixed_size_list_elements = [
    (f, data)
    for f, data in [*primitive_cols, *nullable_primitives]
    if f.name.removesuffix("_nullable") in fixed_size_list_element_types
]

fixed_size_list_cols = [
    (
        pa.field(f"fixed_size_list_{f.name}", pa.list_(f, 5), nullable=False),
        [(data * 10)[:5]],
    )
    for f, data in fixed_size_list_elements
]

fixed_size_nullable_list_cols = [
    (
        pa.field(f"fixed_size_list_nullable_{f.name}", pa.list_(f, 5), nullable=True),
        [(data * 10)[:5], None],
    )
    for f, data in fixed_size_list_elements
]

struct_with_two_primitive_cols = [
    (
        pa.field(
            "struct_with_two_primitive_cols",
            pa.struct([pa.field("a", pa.int32()), pa.field("b", pa.float32())]),
        ),
        [{"a": 1, "b": 2}],
    )
]

nested_struct = [
    (
        pa.field(
            "nested_struct",
            pa.struct(
                [
                    pa.field("a", pa.int32()),
                    pa.field("s", pa.struct([pa.field("b", pa.float32())])),
                ]
            ),
        ),
        [{"a": 1, "s": {"b": 2}}],
    )
]

# A composite type whose fields include an array. Postgres validates the per-field OID
# on binary COPY-in, so this pins the array type OID (`_int4` = 1007) it has to write.
struct_with_list = [
    (
        pa.field(
            "struct_with_list",
            pa.struct(
                [
                    pa.field("a", pa.int32()),
                    pa.field(
                        "b", pa.list_(pa.field("item", pa.int32(), nullable=True))
                    ),
                    pa.field(
                        "c", pa.list_(pa.field("item", pa.string(), nullable=True))
                    ),
                ]
            ),
        ),
        [{"a": 1, "b": [1, 2, 3], "c": ["x", None]}, {"a": 2, "b": [], "c": None}],
    )
]

all_cols = [
    *primitive_cols,
    *nullable_primitives,
    *list_cols,
    *nullable_list_cols,
    *large_list_cols,
    *large_nullable_list_cols,
    *fixed_size_list_cols,
    *fixed_size_nullable_list_cols,
    *struct_with_two_primitive_cols,
    *nested_struct,
    *struct_with_list,
]

tables = {f.name: pa.table([data], schema=pa.schema([f])) for f, data in all_cols}

for name, table in tables.items():
    schema = table.schema
    with (
        pa.OSFile(str(DIR / f"{name}.arrow"), "wb") as sink,
        pa.ipc.new_file(sink, schema=schema) as writer,
    ):
        for batch in table.to_batches():
            writer.write(batch)


template = """\
#[test]
fn test_{case_name}() {{
    run_test_case("{case_name}")
}}
"""


for name in tables:
    print(template.format(case_name=name))
