PYCLASS_TEMPLATE = """\
#[pyclass(module = "pgpq._pgpq")]
struct {name} {{
    inner: encoders::EncoderBuilder,
}}
field_passthrough_impl!({name});
"""

CLASS_TEMPLATE = """\
class {name}:
    def __init__(self, field: pyarrow.Field) -> None:
        ...
"""

field_only = [
    "BooleanEncoderBuilder",
    "UInt8EncoderBuilder",
    "UInt16EncoderBuilder",
    "UInt32EncoderBuilder",
    "Int8EncoderBuilder",
    "Int16EncoderBuilder",
    "Int32EncoderBuilder",
    "Int64EncoderBuilder",
    "Float16EncoderBuilder",
    "Float32EncoderBuilder",
    "Float64EncoderBuilder",
    "TimestampMicrosecondEncoderBuilder",
    "TimestampMillisecondEncoderBuilder",
    "TimestampSecondEncoderBuilder",
    "Date32EncoderBuilder",
    "Time32MillisecondEncoderBuilder",
    "Time32SecondEncoderBuilder",
    "Time64MicrosecondEncoderBuilder",
    "DurationMicrosecondEncoderBuilder",
    "DurationMillisecondEncoderBuilder",
    "DurationSecondEncoderBuilder",
    "StringEncoderBuilder",
    "LargeStringEncoderBuilder",
    "BinaryEncoderBuilder",
    "LargeBinaryEncoderBuilder",
    "ListEncoderBuilder",
    "LargeListEncoderBuilder",
    "DynamicEncoderBuilder",
]

types = [
    "Bool",
    "Bytea",
    "Int8",
    "Int2",
    "Int4",
    "Char",
    "Text",
    "Json",
    "Jsonb",
    "Float4",
    "Float8",
    "Date",
    "Time",
    "Timestamp",
    "Interval",
]

TYPE_TEMPLATE = """\
#[pyclass(module = "pgpq._pgpq")]
struct {name} {{
    inner: PostgresType,
}}
impl_simple!({name}, PostgresType::{name});
"""

PY_TYPE_TEMPLATE = """\
class {name}:
    pass
"""

for name in field_only:
    print(PYCLASS_TEMPLATE.format(name=name))

for name in field_only:
    print(CLASS_TEMPLATE.format(name=name))

for name in types:
    print(TYPE_TEMPLATE.format(name=name))

for name in types:
    print(PY_TYPE_TEMPLATE.format(name=name))
