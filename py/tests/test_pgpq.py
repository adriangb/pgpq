from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, List, Tuple

import pyarrow as pa
import pyarrow.ipc as paipc
import pytest
import polars as pl
from polars.testing import assert_frame_equal
from testing.postgresql import Postgresql
import psycopg

from pgpq import ArrowToPostgresBinaryEncoder, Schema


@pytest.fixture(scope="session")
def postgres():
    return Postgresql()


Connection = psycopg.Connection[Tuple[Any, ...]]


@pytest.fixture(scope="session")
def dbconn(postgres: Postgresql) -> Iterator[Connection]:
    print(postgres.url())
    with psycopg.connect(str(postgres.url())) as conn:
        yield conn


def copy_buffer_and_get_rows(
    schema: Schema, buffer: bytes, dbconn: Connection
) -> List[Tuple[Any, ...]]:
    cols = [f"\"{col['name']}\" {col['data_type']['ddl']}" for col in schema["columns"]]
    ddl = f"CREATE TEMP TABLE data ({','.join(cols)})"
    try:
        with dbconn.cursor() as cursor:
            cursor.execute(ddl)  # type: ignore
            with cursor.copy("COPY data FROM STDIN WITH (FORMAT BINARY)") as copy:
                copy.write(buffer)
            cursor.execute("SELECT * FROM data")
            rows = cursor.fetchall()
    finally:
        dbconn.rollback()  # all that matters is that we drop our temp table
    return rows


@dataclass
class PostgresColumnInfo:
    data_type: str


@dataclass
class ArrowIPCTestCase:
    name: str


PRIMITIVE_TESTCASES = [
    "bool",
    "uint8",
    "uint16",
    "uint32",
    "int8",
    "int16",
    "int32",
    "int64",
    "float32",
    "timestamp_us_notz",
    "timestamp_ms_notz",
    "timestamp_s_notz",
    "timestamp_us_tz",
    "timestamp_ms_tz",
    "timestamp_s_tz",
    "time_us",
    "time_ms",
    "time_s",
    "duration_us",
    "duration_ms",
    "duration_s",
    "binary",
    "large_binary",
    "string",
    "large_string",
]

PRIMITIVE_NULL_TESTCASES = [f"{case}_nullable" for case in PRIMITIVE_TESTCASES]


@pytest.mark.parametrize(
    "testcase",
    [
        *PRIMITIVE_TESTCASES,
        *PRIMITIVE_NULL_TESTCASES,
    ],
)
def test_encode_record_batch(dbconn: Connection, testcase: str) -> None:
    path = Path("core/tests/testdata") / f"{testcase}.arrow"
    arrow_table = paipc.open_file(path).read_all()
    encoder = ArrowToPostgresBinaryEncoder(arrow_table.schema)
    buffer = bytearray()
    buffer.extend(encoder.write_header())
    for batch in arrow_table.to_batches():
        buffer.extend(encoder.write_batch(batch))
    buffer.extend(encoder.finish())

    pg_schema = encoder.schema()

    rows = copy_buffer_and_get_rows(pg_schema, buffer, dbconn)
    col = arrow_table.schema.names[0]
    new_table = pa.Table.from_pylist(
        [{col: row[0]} for row in rows], schema=arrow_table.schema
    )

    original_df: pl.DataFrame = pl.from_arrow(arrow_table)
    new_df: pl.DataFrame = pl.from_arrow(new_table)

    assert_frame_equal(
        original_df,
        new_df,
    )


def test_schema(dbconn: Connection) -> None:
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([0, 1, 2]),
            pa.array([True, False, None]),
            pa.array([None, ["foo"], ["bar"]]),
            pa.array([[""], ["foo", None], ["bar"]]),
        ],
        schema=pa.schema(
            [
                ("int", pa.int32()),
                ("nullable bool", pa.bool_()),
                pa.field(
                    "a nullable list of strings",
                    pa.list_(pa.field("field", pa.string(), nullable=False)),
                    nullable=True,
                ),
                pa.field(
                    "a list of nullable strings",
                    pa.list_(pa.field("field", pa.string(), nullable=True)),
                    nullable=False,
                ),
            ]
        ),
    )

    encoder = ArrowToPostgresBinaryEncoder(batch.schema)
    schema = encoder.schema()

    assert schema == {
        "columns": [
            {
                "name": "int",
                "nullable": True,
                "data_type": {"type": "INT4", "ddl": "INT4"},
            },
            {
                "name": "nullable bool",
                "nullable": True,
                "data_type": {"type": "BOOL", "ddl": "BOOL"},
            },
            {
                "name": "a nullable list of strings",
                "nullable": True,
                "data_type": {
                    "type": "LIST",
                    "inner": {
                        "name": "field",
                        "nullable": False,
                        "data_type": {"type": "TEXT", "ddl": "TEXT"},
                    },
                    "ddl": "TEXT[]",
                },
            },
            {
                "name": "a list of nullable strings",
                "nullable": False,
                "data_type": {
                    "type": "LIST",
                    "inner": {
                        "name": "field",
                        "nullable": True,
                        "data_type": {"type": "TEXT", "ddl": "TEXT"},
                    },
                    "ddl": "TEXT[]",
                },
            },
        ]
    }
