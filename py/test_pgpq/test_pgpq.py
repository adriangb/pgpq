from __future__ import annotations

from glob import glob
from pathlib import Path
from typing import Any, Iterator, List, Tuple

import pyarrow as pa
import pyarrow.ipc as paipc
import pytest
from testing.postgresql import Postgresql
import psycopg

import pgpq.schema
import pgpq.encoders
from pgpq import ArrowToPostgresBinaryEncoder
from pgpq.schema import PostgresSchema


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
    schema: PostgresSchema, buffer: bytes, dbconn: Connection
) -> List[Tuple[Any, ...]]:
    cols = [f'"{col_name}" {col.data_type.ddl()}' for col_name, col in schema.columns]
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


TESTCASES = sorted(
    [f.strip(".bin").split("/")[-1] for f in sorted(glob("core/tests/snapshots/*"))]
)


@pytest.mark.parametrize(
    "testcase",
    TESTCASES,
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

    assert arrow_table == new_table


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

    assert schema == pgpq.schema.PostgresSchema(
        [
            ("int", pgpq.schema.Column(True, pgpq.schema.Int4())),
            ("nullable bool", pgpq.schema.Column(True, pgpq.schema.Bool())),
            (
                "a nullable list of strings",
                pgpq.schema.Column(
                    True,
                    pgpq.schema.List(
                        pgpq.schema.Column(
                            False,
                            pgpq.schema.Text(),
                        )
                    ),
                ),
            ),
            (
                "a list of nullable strings",
                pgpq.schema.Column(
                    False,
                    pgpq.schema.List(
                        pgpq.schema.Column(
                            True,
                            pgpq.schema.Text(),
                        )
                    ),
                ),
            ),
        ]
    )


def test_infer_encoder() -> None:
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

    encoders = {
        field_name: ArrowToPostgresBinaryEncoder.infer_encoder(
            batch.schema.field(field_name)
        )
        for field_name in batch.schema.names
    }

    assert encoders == {
        "int": pgpq.encoders.Int32EncoderBuilder(batch.schema.field("int")),
        "nullable bool": pgpq.encoders.BooleanEncoderBuilder(
            batch.schema.field("nullable bool")
        ),
        "a nullable list of strings": pgpq.encoders.ListEncoderBuilder.new_with_inner(
            batch.schema.field("a nullable list of strings"),
            pgpq.encoders.StringEncoderBuilder(
                batch.schema.field("a nullable list of strings").type.value_field
            ),
        ),
        "a list of nullable strings": pgpq.encoders.ListEncoderBuilder.new_with_inner(
            batch.schema.field(
                "a list of nullable strings",
            ),
            pgpq.encoders.StringEncoderBuilder(
                batch.schema.field("a list of nullable strings").type.value_field
            ),
        ),
    }


def test_custom_encoding(dbconn: Connection) -> None:
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array(
                [["[]"], ['{"foo":"bar"}'], ["123"]],
                type=pa.list_(pa.field("field", pa.string())),
            ),
        ],
        schema=pa.schema(
            [
                pa.field(
                    "json_list",
                    pa.list_(pa.field("field", pa.string())),
                ),
            ]
        ),
    )

    encoders = {
        "json_list": pgpq.encoders.ListEncoderBuilder.new_with_inner(
            batch.schema.field("json_list"),
            pgpq.encoders.StringEncoderBuilder.new_with_output(
                batch.schema.field("json_list").type.value_field, pgpq.schema.Jsonb()
            ),
        )
    }

    encoder = ArrowToPostgresBinaryEncoder.new_with_encoders(batch.schema, encoders)
    buffer = bytearray()
    buffer.extend(encoder.write_header())
    buffer.extend(encoder.write_batch(batch))
    buffer.extend(encoder.finish())

    pg_schema = encoder.schema()

    rows = copy_buffer_and_get_rows(pg_schema, buffer, dbconn)
    assert rows == [([[]],), ([{"foo": "bar"}],), ([123],)]
