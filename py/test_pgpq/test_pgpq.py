"""Binding layer tests for the ``pgpq`` Python extension.

Value level correctness for every supported Arrow type is covered by the Rust
integration suite (``core/tests/integration_tests.rs::validate_roundtrip_values``),
which pushes every generated test case through embedded Postgres and compares typed
decoded values against the source Arrow arrays. There is no reason to duplicate that
~250 case sweep here.

What this module covers instead is the pyo3 surface:

* a handful of end to end roundtrips (primitive, string, list, struct) proving the
  bindings hand Postgres bytes it accepts,
* the schema objects (``PostgresSchema``/``Column``) and DDL generation,
* encoder inference and the encoder builder classes, including a custom output type,
* translation of Rust errors into Python exceptions,
* a light ``hypothesis`` roundtrip over arbitrary primitive tables, so the binding layer
  itself sees generated input rather than only hand written fixtures.

The fixtures are built inline with pyarrow, so these tests do not read anything from
the repository and can run from any working directory (including against an installed
wheel).
"""

from __future__ import annotations

import math
from collections.abc import Iterator
from typing import Any

import pgpq.encoders
import pgpq.schema
import psycopg
import pyarrow as pa
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pgpq import ArrowToPostgresBinaryEncoder
from pgpq._pgpq import Column
from pgpq.schema import PostgresSchema
from testing.postgresql import Postgresql

Connection = psycopg.Connection[tuple[Any, ...]]


@pytest.fixture(scope="session")
def postgres() -> Iterator[Postgresql]:
    with Postgresql() as postgres:
        yield postgres


@pytest.fixture(scope="session")
def dbconn(postgres: Postgresql) -> Iterator[Connection]:
    # `testing.postgresql` may build the cluster with a SQL_ASCII encoding depending
    # on the ambient locale; pinning the client encoding keeps text columns coming
    # back as `str`.
    with psycopg.connect(str(postgres.url()), client_encoding="UTF8") as conn:
        yield conn


def encode(table: pa.Table) -> tuple[PostgresSchema, bytes]:
    """Encode a table with the default (inferred) encoders."""
    encoder = ArrowToPostgresBinaryEncoder(table.schema)
    buffer = bytearray()
    buffer.extend(encoder.write_header())
    for batch in table.to_batches():
        buffer.extend(encoder.write_batch(batch))
    buffer.extend(encoder.finish())
    return encoder.schema(), bytes(buffer)


def copy_buffer_and_get_rows(
    schema: PostgresSchema, buffer: bytes, dbconn: Connection
) -> list[tuple[Any, ...]]:
    ddl = schema.ddl("data")
    try:
        with dbconn.cursor() as cursor:
            cursor.execute(ddl)  # type: ignore[arg-type]
            with cursor.copy("COPY data FROM STDIN WITH (FORMAT BINARY)") as copy:
                copy.write(buffer)
            cursor.execute("SELECT * FROM data")
            rows = cursor.fetchall()
    finally:
        dbconn.rollback()  # all that matters is that we drop our temp table
    return rows


def roundtrip(table: pa.Table, dbconn: Connection) -> list[tuple[Any, ...]]:
    schema, buffer = encode(table)
    return copy_buffer_and_get_rows(schema, buffer, dbconn)


# --------------------------------------------------------------------------------------
# Roundtrip smoke tests, one per category of Arrow type
# --------------------------------------------------------------------------------------


def test_roundtrip_primitives(dbconn: Connection) -> None:
    table = pa.table(
        {
            "int": pa.array([-1, 0, None], pa.int32()),
            "big": pa.array([1, 2, None], pa.int64()),
            "real": pa.array([1.5, -2.5, None], pa.float64()),
            "flag": pa.array([True, False, None], pa.bool_()),
        }
    )

    rows = roundtrip(table, dbconn)

    assert rows == [
        (-1, 1, 1.5, True),
        (0, 2, -2.5, False),
        (None, None, None, None),
    ]


def test_roundtrip_strings_and_binary(dbconn: Connection) -> None:
    table = pa.table(
        {
            "s": pa.array(["", "some data! ", None], pa.string()),
            "large": pa.array(["", "some large string", None], pa.large_string()),
            "b": pa.array([b"", b"\x00\x01\x02", None], pa.binary()),
        }
    )

    rows = roundtrip(table, dbconn)

    assert rows == [
        ("", "", b""),
        ("some data! ", "some large string", b"\x00\x01\x02"),
        (None, None, None),
    ]


def test_roundtrip_lists(dbconn: Connection) -> None:
    table = pa.table(
        {
            "ints": pa.array([[1, 2, 3], [], None], pa.list_(pa.int32())),
            "strings": pa.array(
                [["foo", None], ["bar"], None],
                pa.list_(pa.field("item", pa.string(), nullable=True)),
            ),
        }
    )

    rows = roundtrip(table, dbconn)

    assert rows == [
        ([1, 2, 3], ["foo", None]),
        ([], ["bar"]),
        (None, None),
    ]


def test_roundtrip_structs(dbconn: Connection) -> None:
    """Structs become Postgres composite types.

    psycopg has no registered adapter for the generated composite type, so it hands
    back Postgres' text representation. That is enough for a binding smoke test; the
    Rust suite decodes composites field by field.
    """
    struct_type = pa.struct(
        [pa.field("num", pa.int32()), pa.field("text", pa.string())]
    )
    nested_type = pa.struct(
        [
            pa.field("a", pa.int32()),
            pa.field("s", pa.struct([pa.field("b", pa.int32())])),
        ]
    )
    table = pa.table(
        {
            "flat": pa.array([{"num": 1, "text": "a"}, None], struct_type),
            "nested": pa.array(
                [{"a": 1, "s": {"b": 2}}, {"a": 3, "s": None}], nested_type
            ),
        }
    )

    rows = roundtrip(table, dbconn)

    assert rows == [
        ("(1,a)", '(1,"(2)")'),
        (None, "(3,)"),
    ]


def test_roundtrip_fixed_size_types(dbconn: Connection) -> None:
    """``FixedSizeBinary`` lands in a BYTEA and ``FixedSizeList`` in an array."""
    table = pa.table(
        {
            "b": pa.array([b"abc", None], pa.binary(3)),
            "l": pa.array(
                [[1, 2], None],
                pa.list_(pa.field("item", pa.int32(), nullable=True), 2),
            ),
        }
    )

    rows = roundtrip(table, dbconn)

    assert rows == [(b"abc", [1, 2]), (None, None)]


def test_roundtrip_struct_with_list_field(dbconn: Connection) -> None:
    """A composite type with an array column (issue #90).

    Postgres checks the OID pgpq writes for each composite field against the column's
    declared type, so this only loads if the array type OID is the real one.
    """
    struct_type = pa.struct(
        [
            pa.field("num", pa.int32()),
            pa.field("nums", pa.list_(pa.field("item", pa.int32(), nullable=True))),
        ]
    )
    table = pa.table({"s": pa.array([{"num": 1, "nums": [1, 2]}, None], struct_type)})

    rows = roundtrip(table, dbconn)

    assert rows == [('(1,"{1,2}")',), (None,)]


def test_struct_with_list_of_structs_raises_value_error() -> None:
    """The one composite shape that has no answer: an array of a user defined type.

    Its array type OID is assigned when the type is created, so pgpq cannot know it.
    """
    schema = pa.schema(
        [
            pa.field(
                "s",
                pa.struct(
                    [
                        pa.field(
                            "structs",
                            pa.list_(pa.struct([pa.field("num", pa.int32())])),
                        )
                    ]
                ),
            )
        ]
    )

    with pytest.raises(ValueError):
        ArrowToPostgresBinaryEncoder(schema)


def test_roundtrip_custom_encoding_to_jsonb(dbconn: Connection) -> None:
    """A string column can be told to land in Postgres as JSONB instead of TEXT."""
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array(
                [["[]"], ['{"foo":"bar"}'], ["123"]],
                type=pa.list_(pa.field("field", pa.string())),
            ),
        ],
        schema=pa.schema(
            [pa.field("json_list", pa.list_(pa.field("field", pa.string())))]
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

    rows = copy_buffer_and_get_rows(encoder.schema(), bytes(buffer), dbconn)
    assert rows == [([[]],), ([{"foo": "bar"}],), ([123],)]


# --------------------------------------------------------------------------------------
# Property based roundtrip
# --------------------------------------------------------------------------------------
#
# The heavy generative work lives in Rust (``core/tests/proptest_roundtrip.rs``), which
# generates every supported Arrow type against embedded Postgres. This is the light
# equivalent whose job is to keep the *pyo3 boundary* in the loop: arbitrary pyarrow
# tables of mixed primitive columns, encoded through the Python bindings and pushed
# through the same Postgres fixture as the tests above.


#: Column types and the values to fill them with. Deliberately primitive: decimals are
#: excluded because the Rust suite documents open encoder bugs there, and ``\x00`` is
#: excluded from text because Postgres rejects it in ``text`` regardless of pgpq.
_COLUMN_TYPES: list[tuple[pa.DataType, st.SearchStrategy[Any]]] = [
    (pa.int16(), st.integers(min_value=-(2**15), max_value=2**15 - 1)),
    (pa.int32(), st.integers(min_value=-(2**31), max_value=2**31 - 1)),
    (pa.int64(), st.integers(min_value=-(2**63), max_value=2**63 - 1)),
    (pa.float64(), st.floats(width=64)),
    (pa.bool_(), st.booleans()),
    (
        pa.string(),
        st.text(alphabet=st.characters(blacklist_characters="\x00"), max_size=32),
    ),
    (pa.large_binary(), st.binary(max_size=32)),
]


@st.composite
def _mixed_tables(draw: st.DrawFn) -> pa.Table:
    """A small table of 1-4 primitive columns, with nulls and the zero row edge case."""
    num_rows = draw(st.integers(min_value=0, max_value=6))
    num_columns = draw(st.integers(min_value=1, max_value=4))
    columns = {}
    for i in range(num_columns):
        arrow_type, values = draw(st.sampled_from(_COLUMN_TYPES))
        column = draw(
            st.lists(st.none() | values, min_size=num_rows, max_size=num_rows)
        )
        columns[f"c{i}"] = pa.array(column, arrow_type)
    return pa.table(columns)


def _values_equal(expected: Any, actual: Any) -> bool:
    # NaN != NaN, but "Postgres gave back the NaN that went in" is what we assert.
    if isinstance(expected, float) and isinstance(actual, float):
        return expected == actual or (math.isnan(expected) and math.isnan(actual))
    return bool(expected == actual)


@settings(max_examples=25, deadline=None)
@given(table=_mixed_tables())
def test_roundtrip_arbitrary_primitive_tables(
    dbconn: Connection, table: pa.Table
) -> None:
    rows = roundtrip(table, dbconn)

    expected = list(zip(*(column.to_pylist() for column in table.columns), strict=True))
    assert len(rows) == table.num_rows
    for actual_row, expected_row in zip(rows, expected, strict=True):
        assert len(actual_row) == len(expected_row)
        for actual, expected_value in zip(actual_row, expected_row, strict=True):
            assert _values_equal(expected_value, actual), (
                f"{expected_value!r} != {actual!r} in {table.schema}"
            )


# --------------------------------------------------------------------------------------
# API surface
# --------------------------------------------------------------------------------------


def _mixed_schema() -> pa.Schema:
    return pa.schema(
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
    )


def test_schema() -> None:
    encoder = ArrowToPostgresBinaryEncoder(_mixed_schema())

    assert encoder.schema() == pgpq.schema.PostgresSchema(
        [
            pgpq.schema.Column("int", True, pgpq.schema.Int4()),
            pgpq.schema.Column("nullable bool", True, pgpq.schema.Bool()),
            pgpq.schema.Column(
                "a nullable list of strings",
                True,
                pgpq.schema.List(
                    pgpq.schema.Column("field", False, pgpq.schema.Text())
                ),
            ),
            pgpq.schema.Column(
                "a list of nullable strings",
                False,
                pgpq.schema.List(pgpq.schema.Column("field", True, pgpq.schema.Text())),
            ),
        ]
    )


def test_ddl() -> None:
    encoder = ArrowToPostgresBinaryEncoder(_mixed_schema())
    schema = encoder.schema()

    # `temp_table` defaults to True
    assert schema.ddl("my_table", False) == (
        'CREATE TABLE "my_table" ('
        '"int" INT4, '
        '"nullable bool" BOOL, '
        '"a nullable list of strings" TEXT[], '
        '"a list of nullable strings" TEXT[] NOT NULL);'
    )
    assert schema.ddl("my_table").startswith('CREATE TEMP TABLE "my_table"')


def test_ddl_creates_types_for_structs() -> None:
    schema = pa.schema(
        [
            pa.field(
                "my_struct",
                pa.struct([pa.field("num", pa.int32()), pa.field("text", pa.string())]),
            )
        ]
    )

    ddl = ArrowToPostgresBinaryEncoder(schema).schema().ddl("my_table", False)

    assert ddl == (
        'CREATE TYPE my_struct_t AS ("f0" INT4, "f1" TEXT);\n'
        'CREATE TABLE "my_table" ("my_struct" my_struct_t);'
    )


def test_infer_encoder() -> None:
    schema = _mixed_schema()

    encoders = {
        name: ArrowToPostgresBinaryEncoder.infer_encoder(schema.field(name))
        for name in schema.names
    }

    assert encoders == {
        "int": pgpq.encoders.Int32EncoderBuilder(schema.field("int")),
        "nullable bool": pgpq.encoders.BooleanEncoderBuilder(
            schema.field("nullable bool")
        ),
        "a nullable list of strings": pgpq.encoders.ListEncoderBuilder.new_with_inner(
            schema.field("a nullable list of strings"),
            pgpq.encoders.StringEncoderBuilder(
                schema.field("a nullable list of strings").type.value_field
            ),
        ),
        "a list of nullable strings": pgpq.encoders.ListEncoderBuilder.new_with_inner(
            schema.field("a list of nullable strings"),
            pgpq.encoders.StringEncoderBuilder(
                schema.field("a list of nullable strings").type.value_field
            ),
        ),
    }


def test_infer_encoder_fixed_size_types() -> None:
    schema = pa.schema(
        [
            pa.field("b", pa.binary(3)),
            pa.field("l", pa.list_(pa.field("item", pa.int32(), nullable=True), 2)),
        ]
    )

    encoders = {
        name: ArrowToPostgresBinaryEncoder.infer_encoder(schema.field(name))
        for name in schema.names
    }

    assert encoders == {
        "b": pgpq.encoders.FixedSizeBinaryEncoderBuilder(schema.field("b")),
        "l": pgpq.encoders.FixedSizeListEncoderBuilder.new_with_inner(
            schema.field("l"),
            pgpq.encoders.Int32EncoderBuilder(schema.field("l").type.value_field),
        ),
    }
    # `repr` of a list builder names its own class rather than always saying "List".
    assert repr(encoders["l"]).startswith("FixedSizeListEncoderBuilder(")


def test_column_properties() -> None:
    column = Column("col", False, pgpq.schema.Text())
    assert column.name == "col"
    assert not column.nullable
    assert column.data_type == pgpq.schema.Text()


# --------------------------------------------------------------------------------------
# Error translation
# --------------------------------------------------------------------------------------


def test_unsupported_arrow_type_raises_value_error() -> None:
    schema = pa.schema([pa.field("m", pa.map_(pa.string(), pa.int32()))])

    with pytest.raises(ValueError):
        ArrowToPostgresBinaryEncoder(schema)


def test_missing_encoder_raises_value_error() -> None:
    schema = pa.schema([("a", pa.int32()), ("b", pa.int32())])
    encoders = {"a": ArrowToPostgresBinaryEncoder.infer_encoder(schema.field("a"))}

    with pytest.raises(ValueError):
        ArrowToPostgresBinaryEncoder.new_with_encoders(schema, encoders)


def test_unknown_encoder_field_raises_value_error() -> None:
    schema = pa.schema([("a", pa.int32())])
    encoders = {
        "a": ArrowToPostgresBinaryEncoder.infer_encoder(schema.field("a")),
        "nope": ArrowToPostgresBinaryEncoder.infer_encoder(schema.field("a")),
    }

    with pytest.raises(ValueError):
        ArrowToPostgresBinaryEncoder.new_with_encoders(schema, encoders)


def test_invalid_output_type_raises_value_error() -> None:
    field = pa.field("s", pa.string())

    with pytest.raises(ValueError):
        pgpq.encoders.StringEncoderBuilder.new_with_output(field, pgpq.schema.Int4())


def test_write_batch_with_mismatched_schema_raises_value_error() -> None:
    encoder = ArrowToPostgresBinaryEncoder(pa.schema([("a", pa.int32())]))
    encoder.write_header()
    other = pa.record_batch({"a": pa.array(["x"], pa.string())})

    with pytest.raises(ValueError):
        encoder.write_batch(other)
