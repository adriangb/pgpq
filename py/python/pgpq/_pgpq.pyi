from typing import Mapping, Protocol, Union

import pyarrow

class Bool:
    pass

class Bytea:
    pass

class Int8:
    pass

class Int2:
    pass

class Int4:
    pass

class Char:
    pass

class Text:
    pass

class Jsonb:
    pass

class Float4:
    pass

class Float8:
    pass

class Date:
    pass

class Time:
    pass

class Timestamp:
    pass

class Interval:
    pass

class List:
    def __init__(self, __type: PostgresType) -> None: ...

class Column:
    def __init__(self, __nullable: bool, __type: PostgresType) -> None: ...

class PostgresSchema(Protocol):
    columns: list[tuple[str, Column]]

PostgresType = Union[
    Bool,
    Bytea,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Char,
    Text,
    Jsonb,
    Date,
    Time,
    Timestamp,
    Interval,
    List,
]

class ArrowToPostgresBinaryEncoder:
    def __init__(self, __schema: pyarrow.Schema) -> None: ...
    @staticmethod
    def new_with_encoders(
        __schema: pyarrow.Schema, __encoders: Mapping[str, EncoderBuilder]
    ) -> ArrowToPostgresBinaryEncoder: ...
    def write_header(self) -> bytes: ...
    def write_batch(self, __batch: pyarrow.RecordBatch) -> bytes: ...
    def finish(self) -> bytes: ...
    def schema(self) -> PostgresSchema: ...
    @staticmethod
    def infer_encoder(__field: pyarrow.Field) -> EncoderBuilder: ...

class BooleanEncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...

class UInt8EncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...

class UInt16EncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...

class UInt32EncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...

class Int8EncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...
    @classmethod
    def new_with_output(
        cls, field: pyarrow.Field, output: Char | Int2
    ) -> Int8EncoderBuilder: ...

class Int16EncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...

class Int32EncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...

class Int64EncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...

class Float16EncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...

class Float32EncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...

class Float64EncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...

class TimestampMicrosecondEncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...

class TimestampMillisecondEncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...

class TimestampSecondEncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...

class Date32EncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...

class Date64EncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...

class Time32MillisecondEncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...

class Time32SecondEncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...

class Time64MicrosecondEncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...

class DurationMicrosecondEncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...

class DurationMillisecondEncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...

class DurationSecondEncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...

class StringEncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...
    @classmethod
    def new_with_output(
        cls, field: pyarrow.Field, output: Text | Jsonb
    ) -> Int8EncoderBuilder: ...

class LargeStringEncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...
    @classmethod
    def new_with_output(
        cls, field: pyarrow.Field, output: Text | Jsonb
    ) -> Int8EncoderBuilder: ...

class BinaryEncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...

class LargeBinaryEncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...

class ListEncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...
    @classmethod
    def new_with_inner(
        cls, field: pyarrow.Field, inner_encoder_builder: EncoderBuilder
    ) -> ListEncoderBuilder: ...

class LargeListEncoderBuilder:
    def __init__(self, field: pyarrow.Field) -> None: ...
    @classmethod
    def new_with_inner(
        cls, field: pyarrow.Field, inner_encoder_builder: EncoderBuilder
    ) -> LargeListEncoderBuilder: ...

EncoderBuilder = (
    BooleanEncoderBuilder
    | UInt8EncoderBuilder
    | UInt16EncoderBuilder
    | UInt32EncoderBuilder
    | Int8EncoderBuilder
    | Int16EncoderBuilder
    | Int32EncoderBuilder
    | Int64EncoderBuilder
    | Float16EncoderBuilder
    | Float32EncoderBuilder
    | Float64EncoderBuilder
    | TimestampMicrosecondEncoderBuilder
    | TimestampMillisecondEncoderBuilder
    | TimestampSecondEncoderBuilder
    | Date32EncoderBuilder
    | Date64EncoderBuilder
    | Time32MillisecondEncoderBuilder
    | Time32SecondEncoderBuilder
    | Time64MicrosecondEncoderBuilder
    | DurationMicrosecondEncoderBuilder
    | DurationMillisecondEncoderBuilder
    | DurationSecondEncoderBuilder
    | StringEncoderBuilder
    | LargeStringEncoderBuilder
    | BinaryEncoderBuilder
    | LargeBinaryEncoderBuilder
    | ListEncoderBuilder
    | LargeListEncoderBuilder
)
