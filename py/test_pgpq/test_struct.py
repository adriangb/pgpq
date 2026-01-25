"""Tests for struct (composite type) support."""

from __future__ import annotations

import ast
from typing import Any, Iterator, List, Tuple

import pyarrow as pa
import pytest
from testing.postgresql import Postgresql
import psycopg

from pgpq import ArrowToPostgresBinaryEncoder
from pgpq.schema import PostgresSchema


Connection = psycopg.Connection[Tuple[Any, ...]]


@pytest.fixture(scope="session")
def postgres():
    return Postgresql()


@pytest.fixture(scope="session")
def dbconn(postgres: Postgresql) -> Iterator[Connection]:
    with psycopg.connect(str(postgres.url())) as conn:
        yield conn


def parse_composite_value(value: str, field: pa.Field) -> Any:
    """Parse Postgres composite type (struct) or array to Python tuple/list."""
    if value is None:
        return None

    # Handle arrays (lists)
    if pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
        if not value.startswith("{") or not value.endswith("}"):
            return value
        # Remove outer braces
        s = value[1:-1]
        if not s:  # Empty array
            return []

        # Parse array elements, handling nested braces and quotes
        elements = []
        depth = 0
        in_quotes = False
        start = 0
        for i, c in enumerate(s):
            if c == '"' and (i == 0 or s[i - 1] != "\\"):
                in_quotes = not in_quotes
            elif not in_quotes:
                if c in ("{", "("):
                    depth += 1
                elif c in ("}", ")"):
                    depth -= 1
                elif c == "," and depth == 0:
                    elem = s[start:i].strip()
                    # Remove quotes if present
                    if elem.startswith('"') and elem.endswith('"'):
                        elem = elem[1:-1]
                    elements.append(elem)
                    start = i + 1
        # Last element
        elem = s[start:].strip()
        if elem.startswith('"') and elem.endswith('"'):
            elem = elem[1:-1]
        elements.append(elem)

        # Recursively parse each element
        element_field = field.type.value_field
        return [
            parse_composite_value(e, element_field) if e else None for e in elements
        ]

    # Handle structs
    if not pa.types.is_struct(field.type):
        return value

    # Remove outer parentheses
    s = value[1:-1]
    # Parse fields, handling nested parentheses
    fields = []
    depth = 0
    start = 0
    for i, c in enumerate(s):
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif c == "," and depth == 0:
            fields.append(s[start:i])
            start = i + 1
    fields.append(s[start:])  # last field

    struct_fields = field.type
    result = []
    for f, v in zip(struct_fields, fields):
        v = v if v != "" else None
        if v is None:
            result.append(None)
        elif pa.types.is_struct(f.type):
            result.append(parse_composite_value(v, f))
        else:
            # Parse boolean 't'/'f' and other values
            if v == "t":
                result.append(True)
            elif v == "f":
                result.append(False)
            else:
                try:
                    result.append(ast.literal_eval(v))
                except (ValueError, SyntaxError):
                    result.append(v)
    return tuple(result)


def copy_buffer_and_get_rows(
    schema: PostgresSchema, buffer: bytes, dbconn: Connection
) -> List[Tuple[Any, ...]]:
    ddl = schema.ddl("data")
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


def test_basic_struct(dbconn: Connection) -> None:
    """Test encoding a basic struct with int and string fields."""
    # Create a struct array with int and string fields
    int_array = pa.array([1, 2, 3])
    string_array = pa.array(["a", "b", "c"])
    struct_array = pa.StructArray.from_arrays(
        [int_array, string_array], names=["num", "text"]
    )

    batch = pa.RecordBatch.from_arrays(
        [struct_array],
        schema=pa.schema(
            [
                pa.field(
                    "my_struct",
                    pa.struct(
                        [pa.field("num", pa.int32()), pa.field("text", pa.string())]
                    ),
                )
            ]
        ),
    )

    encoder = ArrowToPostgresBinaryEncoder(batch.schema)
    buffer = bytearray()
    buffer.extend(encoder.write_header())
    buffer.extend(encoder.write_batch(batch))
    buffer.extend(encoder.finish())

    pg_schema = encoder.schema()
    rows = copy_buffer_and_get_rows(pg_schema, buffer, dbconn)

    # Parse composite types from Postgres text representation
    struct_field = batch.schema.field(0)
    parsed_rows = [(parse_composite_value(row[0], struct_field),) for row in rows]

    # Verify the data roundtrips correctly
    assert len(parsed_rows) == 3
    assert parsed_rows[0] == ((1, "a"),)
    assert parsed_rows[1] == ((2, "b"),)
    assert parsed_rows[2] == ((3, "c"),)


def test_nullable_struct(dbconn: Connection) -> None:
    """Test encoding a struct column with null values."""
    int_array = pa.array([1, None, 3])
    string_array = pa.array(["a", None, "c"])

    # Create struct array with some null entries
    struct_array = pa.StructArray.from_arrays(
        [int_array, string_array],
        names=["num", "text"],
        mask=pa.array([False, True, False]),  # Second struct is null
    )

    batch = pa.RecordBatch.from_arrays(
        [struct_array],
        schema=pa.schema(
            [
                pa.field(
                    "my_struct",
                    pa.struct(
                        [
                            pa.field("num", pa.int32(), nullable=True),
                            pa.field("text", pa.string(), nullable=True),
                        ]
                    ),
                    nullable=True,
                )
            ]
        ),
    )

    encoder = ArrowToPostgresBinaryEncoder(batch.schema)
    buffer = bytearray()
    buffer.extend(encoder.write_header())
    buffer.extend(encoder.write_batch(batch))
    buffer.extend(encoder.finish())

    pg_schema = encoder.schema()
    rows = copy_buffer_and_get_rows(pg_schema, buffer, dbconn)

    # Parse composite types from Postgres text representation
    struct_field = batch.schema.field(0)
    parsed_rows = [
        (parse_composite_value(row[0], struct_field) if row[0] is not None else None,)
        for row in rows
    ]

    assert len(parsed_rows) == 3
    assert parsed_rows[0] == ((1, "a"),)
    assert parsed_rows[1] == (None,)  # Null struct
    assert parsed_rows[2] == ((3, "c"),)


def test_nested_struct_fields(dbconn: Connection) -> None:
    """Test encoding a struct with various field types."""
    int_array = pa.array([1, 2])
    float_array = pa.array([1.5, 2.5])
    bool_array = pa.array([True, False])

    struct_array = pa.StructArray.from_arrays(
        [int_array, float_array, bool_array],
        names=["int_field", "float_field", "bool_field"],
    )

    batch = pa.RecordBatch.from_arrays(
        [struct_array],
        schema=pa.schema(
            [
                pa.field(
                    "complex_struct",
                    pa.struct(
                        [
                            pa.field("int_field", pa.int32()),
                            pa.field("float_field", pa.float64()),
                            pa.field("bool_field", pa.bool_()),
                        ]
                    ),
                )
            ]
        ),
    )

    encoder = ArrowToPostgresBinaryEncoder(batch.schema)
    buffer = bytearray()
    buffer.extend(encoder.write_header())
    buffer.extend(encoder.write_batch(batch))
    buffer.extend(encoder.finish())

    pg_schema = encoder.schema()
    rows = copy_buffer_and_get_rows(pg_schema, buffer, dbconn)

    # Parse composite types from Postgres text representation
    struct_field = batch.schema.field(0)
    parsed_rows = [(parse_composite_value(row[0], struct_field),) for row in rows]

    assert len(parsed_rows) == 2
    assert parsed_rows[0] == ((1, 1.5, True),)
    assert parsed_rows[1] == ((2, 2.5, False),)


def test_array_of_structs(dbconn: Connection) -> None:
    """Test encoding arrays of structs (composite_type[]).

    PostgreSQL fully supports arrays of composite types in COPY BINARY format.
    This test verifies lists containing MULTIPLE structs per row.
    """
    # Create structs: (1, "a"), (2, "b"), (3, "c"), (4, "d")
    int_array = pa.array([1, 2, 3, 4])
    string_array = pa.array(["a", "b", "c", "d"])
    struct_array = pa.StructArray.from_arrays(
        [int_array, string_array], names=["num", "text"]
    )

    # Create a list of structs with multiple structs per row
    # Row 0: structs 0-2 -> [(1, "a"), (2, "b"), (3, "c")]
    # Row 1: structs 2-4 -> [(3, "c"), (4, "d")]
    list_of_structs = pa.ListArray.from_arrays(
        pa.array([0, 3, 4]),  # offsets: 3 structs in first row, 1 struct in second row
        struct_array,
    )

    batch = pa.RecordBatch.from_arrays(
        [list_of_structs],
        schema=pa.schema(
            [
                pa.field(
                    "struct_array",
                    pa.list_(
                        pa.field(
                            "item",
                            pa.struct(
                                [
                                    pa.field("num", pa.int32()),
                                    pa.field("text", pa.string()),
                                ]
                            ),
                        )
                    ),
                )
            ]
        ),
    )

    # Encode and send to PostgreSQL
    try:
        encoder = ArrowToPostgresBinaryEncoder(batch.schema)
        buffer = bytearray()
        buffer.extend(encoder.write_header())
        buffer.extend(encoder.write_batch(batch))
        buffer.extend(encoder.finish())

        pg_schema = encoder.schema()
        rows = copy_buffer_and_get_rows(pg_schema, buffer, dbconn)

        # Verify the data roundtrips correctly
        assert len(rows) == 2
        list_field = batch.schema.field(0)
        parsed_rows = [(parse_composite_value(row[0], list_field),) for row in rows]

        # First row should have list with THREE structs: [(1, "a"), (2, "b"), (3, "c")]
        # Second row should have list with ONE struct: [(4, "d")]
        assert parsed_rows[0] == (
            [(1, "a"), (2, "b"), (3, "c")],
        ), f"Expected ([(1, 'a'), (2, 'b'), (3, 'c')],), got {parsed_rows[0]}"
        assert parsed_rows[1] == (
            [(4, "d")],
        ), f"Expected ([(4, 'd')],), got {parsed_rows[1]}"

        print(
            f"✓ Arrays of structs work! "
            f"Row 1: {parsed_rows[0]}, Row 2: {parsed_rows[1]}"
        )
    except Exception as e:
        # If this fails, it's a bug in the encoder implementation
        # PostgreSQL itself fully supports arrays of composite types
        pytest.fail(f"Array of structs should be supported: {type(e).__name__}: {e}")
