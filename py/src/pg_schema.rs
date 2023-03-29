use crate::utils::PythonRepr;
use pyo3::prelude::*;

macro_rules! impl_simple {
    ($struct:ident, $pg_type:path) => {
        #[pymethods]
        impl $struct {
            #[new]
            fn new() -> Self {
                Self {}
            }
            fn __repr__(&self, py: Python) -> String {
                self.py_repr(py)
            }
            fn __str__(&self, py: Python) -> String {
                self.__repr__(py)
            }
        }
        impl From<$struct> for pgpq::pg_schema::PostgresType {
            fn from(_val: $struct) -> Self {
                $pg_type
            }
        }
        impl PythonRepr for $struct {
            fn py_repr(&self, _py: Python) -> String {
                format!("{}()", stringify!($struct))
            }
        }
    };
}

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct Bool;
impl_simple!(Bool, pgpq::pg_schema::PostgresType::Bool);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct Bytea;
impl_simple!(Bytea, pgpq::pg_schema::PostgresType::Bytea);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct Int8;
impl_simple!(Int8, pgpq::pg_schema::PostgresType::Int8);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct Int2;
impl_simple!(Int2, pgpq::pg_schema::PostgresType::Int2);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct Int4;
impl_simple!(Int4, pgpq::pg_schema::PostgresType::Int4);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct Char;
impl_simple!(Char, pgpq::pg_schema::PostgresType::Char);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct Text;
impl_simple!(Text, pgpq::pg_schema::PostgresType::Text);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct Jsonb;
impl_simple!(Jsonb, pgpq::pg_schema::PostgresType::Jsonb);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct Float4;
impl_simple!(Float4, pgpq::pg_schema::PostgresType::Float4);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct Float8;
impl_simple!(Float8, pgpq::pg_schema::PostgresType::Float8);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct Date;
impl_simple!(Date, pgpq::pg_schema::PostgresType::Date);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct Time;
impl_simple!(Time, pgpq::pg_schema::PostgresType::Time);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct Timestamp;
impl_simple!(Timestamp, pgpq::pg_schema::PostgresType::Timestamp);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct Interval;
impl_simple!(Interval, pgpq::pg_schema::PostgresType::Interval);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct List {
    inner: Box<Column>,
}

#[pymethods]
impl List {
    #[new]
    fn new(inner: Column) -> Self {
        Self {
            inner: Box::new(inner),
        }
    }
}

impl From<List> for pgpq::pg_schema::PostgresType {
    fn from(val: List) -> Self {
        pgpq::pg_schema::PostgresType::List(Box::new((*val.inner).into()))
    }
}

impl PythonRepr for List {
    fn py_repr(&self, py: Python) -> String {
        let py_inner = (*self.inner).clone();
        format!("List({})", py_inner.py_repr(py))
    }
}

#[derive(FromPyObject, Debug, Clone)]
pub enum PostgresType {
    Bool(Bool),
    Bytea(Bytea),
    Int2(Int2),
    Int4(Int4),
    Int8(Int8),
    Char(Char),
    Text(Text),
    Jsonb(Jsonb),
    Float4(Float4),
    Float8(Float8),
    Date(Date),
    Time(Time),
    Timestamp(Timestamp),
    Interval(Interval),
    List(List),
}

impl From<PostgresType> for pgpq::pg_schema::PostgresType {
    fn from(value: PostgresType) -> Self {
        match value {
            PostgresType::Bool(inner) => inner.into(),
            PostgresType::Bytea(inner) => inner.into(),
            PostgresType::Int2(inner) => inner.into(),
            PostgresType::Int4(inner) => inner.into(),
            PostgresType::Int8(inner) => inner.into(),
            PostgresType::Char(inner) => inner.into(),
            PostgresType::Text(inner) => inner.into(),
            PostgresType::Jsonb(inner) => inner.into(),
            PostgresType::Float4(inner) => inner.into(),
            PostgresType::Float8(inner) => inner.into(),
            PostgresType::Date(inner) => inner.into(),
            PostgresType::Time(inner) => inner.into(),
            PostgresType::Timestamp(inner) => inner.into(),
            PostgresType::Interval(inner) => inner.into(),
            PostgresType::List(inner) => inner.into(),
        }
    }
}

impl From<pgpq::pg_schema::PostgresType> for PostgresType {
    fn from(value: pgpq::pg_schema::PostgresType) -> Self {
        match value {
            pgpq::pg_schema::PostgresType::Bool => PostgresType::Bool(Bool),
            pgpq::pg_schema::PostgresType::Bytea => PostgresType::Bytea(Bytea),
            pgpq::pg_schema::PostgresType::Int2 => PostgresType::Int2(Int2),
            pgpq::pg_schema::PostgresType::Int4 => PostgresType::Int4(Int4),
            pgpq::pg_schema::PostgresType::Int8 => PostgresType::Int8(Int8),
            pgpq::pg_schema::PostgresType::Char => PostgresType::Char(Char),
            pgpq::pg_schema::PostgresType::Text => PostgresType::Text(Text),
            pgpq::pg_schema::PostgresType::Jsonb => PostgresType::Jsonb(Jsonb),
            pgpq::pg_schema::PostgresType::Float4 => PostgresType::Float4(Float4),
            pgpq::pg_schema::PostgresType::Float8 => PostgresType::Float8(Float8),
            pgpq::pg_schema::PostgresType::Date => PostgresType::Date(Date),
            pgpq::pg_schema::PostgresType::Time => PostgresType::Time(Time),
            pgpq::pg_schema::PostgresType::Timestamp => PostgresType::Timestamp(Timestamp),
            pgpq::pg_schema::PostgresType::Interval => PostgresType::Interval(Interval),
            pgpq::pg_schema::PostgresType::List(inner) => {
                PostgresType::List(List::new((*inner).into()))
            }
        }
    }
}

impl PythonRepr for PostgresType {
    fn py_repr(&self, py: Python) -> String {
        match &self {
            PostgresType::Bool(inner) => inner.py_repr(py),
            PostgresType::Bytea(inner) => inner.py_repr(py),
            PostgresType::Int2(inner) => inner.py_repr(py),
            PostgresType::Int4(inner) => inner.py_repr(py),
            PostgresType::Int8(inner) => inner.py_repr(py),
            PostgresType::Char(inner) => inner.py_repr(py),
            PostgresType::Text(inner) => inner.py_repr(py),
            PostgresType::Jsonb(inner) => inner.py_repr(py),
            PostgresType::Float4(inner) => inner.py_repr(py),
            PostgresType::Float8(inner) => inner.py_repr(py),
            PostgresType::Date(inner) => inner.py_repr(py),
            PostgresType::Time(inner) => inner.py_repr(py),
            PostgresType::Timestamp(inner) => inner.py_repr(py),
            PostgresType::Interval(inner) => inner.py_repr(py),
            PostgresType::List(inner) => inner.py_repr(py),
        }
    }
}

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct Column {
    data_type: PostgresType,
    nullable: bool,
}

impl From<pgpq::pg_schema::Column> for Column {
    fn from(value: pgpq::pg_schema::Column) -> Self {
        Self {
            data_type: value.data_type.into(),
            nullable: value.nullable,
        }
    }
}

impl From<Column> for pgpq::pg_schema::Column {
    fn from(value: Column) -> Self {
        pgpq::pg_schema::Column {
            data_type: value.data_type.into(),
            nullable: value.nullable,
        }
    }
}

impl PythonRepr for Column {
    fn py_repr(&self, py: Python) -> String {
        format!(
            "Column(data_type={}, nullable={})",
            self.data_type.py_repr(py),
            self.nullable
        )
    }
}

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct PostgresSchema {
    columns: Vec<(String, Column)>,
}

impl From<pgpq::pg_schema::PostgresSchema> for PostgresSchema {
    fn from(value: pgpq::pg_schema::PostgresSchema) -> Self {
        Self {
            columns: value
                .columns
                .iter()
                .map(|(field_name, col)| (field_name.clone(), col.clone().into()))
                .collect(),
        }
    }
}

impl From<PostgresSchema> for pgpq::pg_schema::PostgresSchema {
    fn from(value: PostgresSchema) -> Self {
        pgpq::pg_schema::PostgresSchema {
            columns: value
                .columns
                .iter()
                .map(|(field_name, col)| (field_name.clone(), col.clone().into()))
                .collect(),
        }
    }
}
