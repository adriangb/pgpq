from __future__ import annotations

import sys
from typing import Sequence, Union

if sys.version_info < (3, 8):
    from typing_extensions import Literal, TypedDict
else:
    from typing import Literal, TypedDict


class DataTypeBase(TypedDict):
    ddl: str


class BoolType(DataTypeBase):
    type: Literal["BOOL"]


class Int2Type(DataTypeBase):
    type: Literal["INT2"]


class Int4Type(DataTypeBase):
    type: Literal["INT4"]


class Int8Type(DataTypeBase):
    type: Literal["INT8"]


class Float4ype(DataTypeBase):
    type: Literal["FLOAT4"]


class Float8ype(DataTypeBase):
    type: Literal["FLOAT8"]


class DateType(DataTypeBase):
    type: Literal["DATE"]


class TimeType(DataTypeBase):
    type: Literal["TIME"]


class TimestampType(DataTypeBase):
    type: Literal["TIMESTAMP"]


class IntervalType(DataTypeBase):
    type: Literal["INTERVAL"]


class TextType(DataTypeBase):
    type: Literal["Text"]


class ByteaType(DataTypeBase):
    type: Literal["BYTEA"]


class ListType(DataTypeBase):
    type: Literal["LIST"]
    inner: Column


DataType = Union[
    BoolType,
    Int2Type,
    Int4Type,
    Int8Type,
    Float4ype,
    Float8ype,
    DateType,
    TimeType,
    TimestampType,
    IntervalType,
    TextType,
    ByteaType,
    ListType,
]


class Column(TypedDict):
    name: str
    data_type: DataTypeBase
    nullable: bool


class Schema(TypedDict):
    columns: Sequence[Column]
