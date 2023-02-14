from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator, List, Sequence, Tuple

import pyarrow as pa
import pyarrow.ipc as paipc
import pytest
import polars as pl
from polars.testing import assert_frame_equal
from testing.postgresql import Postgresql
import psycopg

from pgpq import ArrowToPostgresBinaryEncoder


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
    col_info: PostgresColumnInfo, buffer: bytes, dbconn: Connection
) -> List[Tuple[Any, ...]]:
    try:
        with dbconn.cursor() as cursor:
            for q in col_info.ddl:
                cursor.execute(q)  # type: ignore
            cursor.execute(
                f"CREATE TEMP TABLE data (col {col_info.data_type})"  # type: ignore
            )
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
    ddl: List[str]


@dataclass
class ArrowIPCTestCase:
    name: str
    data_type: str
    ddl: Sequence[str] = ()
    comparator: Callable[[pl.DataFrame, pl.DataFrame], None] = assert_frame_equal


# def assert_timestamp_dfs_equal(left: pl.DataFrame, right: pl.DataFrame) -> None:
#     col = left.columns[0]
#     # compare as ms
#     left = left.with_columns(pl.col(col).dt.truncate(every="1s"))
#     right = right.with_columns(pl.col(col).dt.truncate(every="1s"))
#     assert_frame_equal(left, right)


PRIMITIVE_TESTCASES = [
    ArrowIPCTestCase(
        "bool",
        "BOOLEAN",
    ),
    ArrowIPCTestCase(
        "uint8",
        "SMALLINT",
    ),
    ArrowIPCTestCase(
        "uint16",
        "INT",
    ),
    ArrowIPCTestCase(
        "uint32",
        "BIGINT",
    ),
    ArrowIPCTestCase(
        "int8",
        "SMALLINT",
    ),
    ArrowIPCTestCase(
        "int16",
        "SMALLINT",
    ),
    ArrowIPCTestCase(
        "int32",
        "INT",
    ),
    ArrowIPCTestCase(
        "int64",
        "BIGINT",
    ),
    # ArrowIPCTestCase(
    #     "float16",
    #     "REAL",
    # ),
    ArrowIPCTestCase(
        "float32",
        "REAL",
    ),
    ArrowIPCTestCase(
        "float64",
        "DOUBLE PRECISION",
    ),
    ArrowIPCTestCase(
        "timestamp_us_notz",
        "TIMESTAMP",
    ),
    ArrowIPCTestCase(
        "timestamp_ms_notz",
        "TIMESTAMP",
    ),
    ArrowIPCTestCase(
        "timestamp_s_notz",
        "TIMESTAMP",
    ),
    ArrowIPCTestCase(
        "timestamp_us_tz",
        "TIMESTAMP",
    ),
    ArrowIPCTestCase(
        "timestamp_ms_tz",
        "TIMESTAMP",
    ),
    ArrowIPCTestCase(
        "timestamp_s_tz",
        "TIMESTAMP",
    ),
    ArrowIPCTestCase(
        "time_us",
        "TIME",
    ),
    ArrowIPCTestCase(
        "time_ms",
        "TIME",
    ),
    ArrowIPCTestCase(
        "time_s",
        "TIME",
    ),
    ArrowIPCTestCase(
        "duration_us",
        "INTERVAL",
    ),
    ArrowIPCTestCase(
        "duration_ms",
        "INTERVAL",
    ),
    ArrowIPCTestCase(
        "duration_s",
        "INTERVAL",
    ),
    ArrowIPCTestCase(
        "binary",
        "BYTEA",
    ),
    ArrowIPCTestCase(
        "large_binary",
        "BYTEA",
    ),
    ArrowIPCTestCase(
        "string",
        "TEXT",
    ),
    ArrowIPCTestCase(
        "large_string",
        "TEXT",
    ),
]

PRIMITIVE_NULL_TESTCASES = [
    ArrowIPCTestCase(
        f"{case.name}_nullable",
        case.data_type,
    )
    for case in PRIMITIVE_TESTCASES
]


@pytest.mark.parametrize(
    "testcase",
    [
        *PRIMITIVE_TESTCASES,
        *PRIMITIVE_NULL_TESTCASES,
    ],
    ids=lambda case: case.name,
)
def test_encode_record_batch(dbconn: Connection, testcase: ArrowIPCTestCase) -> None:
    path = Path("pgpq/tests/testdata") / f"{testcase.name}.arrow"
    arrow_table = paipc.open_file(path).read_all()
    encoder = ArrowToPostgresBinaryEncoder(arrow_table.schema)
    buffer = bytearray()
    buffer.extend(encoder.write_header())
    for batch in arrow_table.to_batches():
        buffer.extend(encoder.write_batch(batch))
    buffer.extend(encoder.finish())

    col_info = PostgresColumnInfo(
        data_type=testcase.data_type, ddl=list(testcase.ddl or [])
    )
    rows = copy_buffer_and_get_rows(col_info, buffer, dbconn)
    col = arrow_table.schema.names[0]
    new_table = pa.Table.from_pylist(
        [{col: row[0]} for row in rows], schema=arrow_table.schema
    )

    original_df: pl.DataFrame = pl.from_arrow(arrow_table)
    new_df: pl.DataFrame = pl.from_arrow(new_table)

    testcase.comparator(
        original_df,
        new_df,
    )
