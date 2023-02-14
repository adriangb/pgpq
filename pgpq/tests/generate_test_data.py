from dataclasses import dataclass
from itertools import cycle, islice
from math import floor
from pathlib import Path
from typing import Any, List
import pyarrow as pa


DIR = (Path(__file__).parent / "testdata").resolve()
DIR.mkdir(exist_ok=True)


@dataclass
class Col:
    name: str
    type: pa.DataType
    data: List[Any]
    nullable: bool = False


timestamp_s = 167_614_2874
timestamp_ms = floor(timestamp_s * 1e3)
timestamp_us = floor(timestamp_s * 1e6)

time_s = 24 * 60 * 60 - 1
time_ms = floor(time_s * 1e3)
time_us = floor(time_s * 1e6)

duration_s = 60
duration_ms = floor(duration_s * 1e3)
duration_us = floor(duration_s * 1e6)

string = "some data! "
large_string = str(islice(cycle(string), 2**32 + 1))
binary = string.encode()
large_binary = large_string.encode()

primitive_cols: list[tuple[pa.field, list[Any]]] = [
    (pa.field("bool", pa.bool_()), [True, False]),
    (pa.field("uint8", pa.uint8()), [0, 1, 2]),
    (pa.field("uint16", pa.uint16()), [0, 1, 2]),
    (pa.field("uint32", pa.uint32()), [0, 1, 2]),
    (pa.field("int8", pa.int8()), [-1, 0, 1]),
    (pa.field("int16", pa.int16()), [-1, 0, 1]),
    (pa.field("int32", pa.int32()), [-1, 0, 1]),
    (pa.field("int64", pa.int64()), [-1, 0, 1]),
    # (
    #     pa.field("float16", pa.float16()),
    #     [np.float16(v) for v in [-1, 0, 1, float("inf")]],
    # ),
    (pa.field("float32", pa.float32()), [-1, 0, 1, float("inf")]),
    (pa.field("float64", pa.float64()), [-1, 0, 1, float("inf")]),
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
    (pa.field("duration_us", pa.duration("us")), [0, 1, duration_us]),
    (pa.field("duration_ms", pa.duration("ms")), [0, 1, duration_ms]),
    (pa.field("duration_s", pa.duration("s")), [0, 1, duration_s]),
    (pa.field("binary", pa.binary()), [b"", binary]),
    (pa.field("large_binary", pa.large_binary()), [b"", binary, large_binary]),
    (pa.field("string", pa.string()), ["", string]),
    (pa.field("large_string", pa.large_string()), ["", string, large_string]),
]

longest = max(len(c[1]) for c in primitive_cols)

# add nullable columns
nullable_primitives = [
    (f.with_name(f"{f.name}_nullable").with_nullable(True), [*data, None])
    for f, data in primitive_cols
]

list_cols = [
    (pa.field(f"list_{f.name}", pa.list_(f.type), nullable=f.nullable), [data])
    for f, data in [*primitive_cols, *nullable_primitives]
]

nullable_list_cols = [
    (pa.field(f"list_nullable_{f.name}", f.type, nullable=True), [*data, None])
    for f, data in list_cols
]

all_cols = [
    *primitive_cols,
    *nullable_primitives,
    *list_cols,
    *nullable_list_cols,
]

tables = {f.name: pa.table([data], schema=pa.schema([f])) for f, data in all_cols}

for name, table in tables.items():
    schema = table.schema
    with pa.OSFile(str(DIR / f"{name}.arrow"), "wb") as sink:
        with pa.ipc.new_file(sink, schema=schema) as writer:
            for batch in table.to_batches():
                writer.write(batch)


template = """\
#[test]
fn test_{case_name}() {{
    run_test_case("{case_name}")
}}
"""


for name, table in tables.items():
    print(template.format(case_name=name))
