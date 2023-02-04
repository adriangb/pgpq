from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Tuple
import polars as pl
import pytest
from testing.postgresql import Postgresql
import psycopg

from pgpq import ArrowToPostgresBinaryEncoder


@pytest.fixture(scope="session")
def postgres():
    return Postgresql()


Connection = psycopg.Connection[Tuple[Any, ...]]


@pytest.fixture(scope="session")
def dbconn(postgres: Postgresql) -> Iterator[Connection]:
    with psycopg.connect(str(postgres.url())) as conn:
        yield conn


@dataclass
class Field:
    pg_type: str
    pl_type: pl.PolarsDataType
    data: List[Any]


Schema = Dict[str, Field]


def copy_buffer_and_get_rows(
    schema: Schema, buffer: bytes, dbconn: Connection
) -> List[Tuple[Any, ...]]:
    ddl = ", ".join([f"{name} {field.pg_type}" for name, field in schema.items()])
    try:
        with dbconn.cursor() as cursor:
            cursor.execute(f"CREATE TEMPORARY TABLE data ({ddl})")  # type: ignore
            with cursor.copy("COPY data FROM STDIN WITH (FORMAT BINARY)") as copy:
                copy.write(buffer)
            cursor.execute("SELECT * FROM data")
            rows = cursor.fetchall()
    finally:
        dbconn.rollback()  # all that matters is that we drop our temp table
    return rows


@pytest.mark.parametrize(
    "schema",
    [
        {"int8_col": Field(pg_type="INT2", pl_type=pl.Int8(), data=[-1, 0, 1])},
        {
            "bool_col": Field(
                pg_type="BOOLEAN", pl_type=pl.Boolean(), data=[True, False]
            )
        },
        # nulls
        {
            "int8_col_nullable": Field(
                pg_type="INT2", pl_type=pl.Int8(), data=[-1, 0, 1, None]
            )
        },
        {
            "bool_col_nullable": Field(
                pg_type="BOOLEAN", pl_type=pl.Boolean(), data=[True, False, None]
            )
        },
    ],
    ids=str,
)
def test_encode_record_batch(dbconn: Connection, schema: Schema) -> None:
    data = {name: field.data for name, field in schema.items()}
    df_schema = {name: field.pl_type for name, field in schema.items()}

    df = pl.DataFrame(data, schema=df_schema)
    arrow_table = df.to_arrow()
    encoder = ArrowToPostgresBinaryEncoder(arrow_table.schema)
    buffer = bytearray()
    buffer.extend(encoder.write_header())
    for batch in arrow_table.to_batches():
        buffer.extend(encoder.write_batch(batch))
    buffer.extend(encoder.finish())

    rows = copy_buffer_and_get_rows(schema, buffer, dbconn)
    new_df = pl.DataFrame(rows, schema=df_schema)
    assert new_df.frame_equal(df)
