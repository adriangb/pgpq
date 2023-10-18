use std::sync::Arc;

use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow_schema::Field;
use pyo3::class::basic::CompareOp;
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::PyType;
use pyo3::{exceptions::PyValueError, prelude::*};

use pgpq::encoders::BuildEncoder;

use crate::pg_schema::PostgresType;

macro_rules! impl_passthrough_encoder_builder {
    ($py_class:ident) => {
        #[pymethods]
        impl $py_class {
            #[new]
            fn new(py: Python, py_field: &PyAny) -> PyResult<Self> {
                let field: Field = FromPyArrow::from_pyarrow(py_field)?;
                let inner = match pgpq::encoders::EncoderBuilder::try_new(Arc::new(field)) {
                    Ok(inner) => inner,
                    Err(e) => {
                        return Err(PyValueError::new_err(format!(
                            "Error building {}: {:?}",
                            stringify!($py_class),
                            e
                        )));
                    }
                };
                Ok(Self {
                    field: py_field.to_object(py),
                    inner,
                })
            }
            fn __repr__(&self, py: Python) -> String {
                crate::utils::PythonRepr::py_repr(self, py)
            }
            fn __str__(&self, py: Python) -> String {
                self.__repr__(py)
            }
            fn __richcmp__(
                &self,
                other: &Self,
                op: CompareOp,
                py: Python<'_>,
            ) -> PyResult<PyObject> {
                let res = match op {
                    CompareOp::Eq => (&self.inner == &other.inner).into_py(py),
                    CompareOp::Ne => (&self.inner != &other.inner).into_py(py),
                    _ => py.NotImplemented(),
                };
                Ok(res)
            }
        }
        impl crate::utils::PythonRepr for $py_class {
            fn py_repr(&self, py: Python) -> String {
                format!(
                    "{}({})",
                    stringify!($py_class),
                    &self.field.clone().into_ref(py).repr().unwrap(),
                )
            }
        }
    };
}

macro_rules! impl_passthrough_encoder_builder_variable_output {
    ($py_class:ident, $pgpq_encoder_builder:ty, $pgpq_encoder_builder_enum_variant:path) => {
        #[pymethods]
        impl $py_class {
            #[new]
            fn new(py: Python, py_field: &PyAny) -> PyResult<Self> {
                let field: Field = FromPyArrow::from_pyarrow(py_field)?;
                let inner = match <$pgpq_encoder_builder>::new(Arc::new(field)) {
                    Ok(inner) => inner,
                    Err(e) => {
                        return Err(PyValueError::new_err(format!(
                            "Error building {}: {:?}",
                            stringify!($py_class),
                            e
                        )));
                    }
                };
                let py_output: crate::pg_schema::PostgresType = inner.schema().data_type.into();
                Ok(Self {
                    field: py_field.to_object(py),
                    output: py_output,
                    inner: $pgpq_encoder_builder_enum_variant(inner),
                })
            }
            #[classmethod]
            fn new_with_output(
                cls: &PyType,
                py: Python,
                py_field: &PyAny,
                py_output: PostgresType,
            ) -> PyResult<Self> {
                let field: Field = FromPyArrow::from_pyarrow(py_field)?;
                let output = pgpq::pg_schema::PostgresType::from(py_output.clone());
                let inner = match <$pgpq_encoder_builder>::new_with_output(Arc::new(field), output)
                {
                    Ok(inner) => inner,
                    Err(e) => {
                        return Err(PyValueError::new_err(format!(
                            "Error building {}: {:?}",
                            cls.name()?,
                            e
                        )));
                    }
                };
                Ok(Self {
                    field: py_field.to_object(py),
                    output: py_output,
                    inner: $pgpq_encoder_builder_enum_variant(inner),
                })
            }
            fn __repr__(&self, py: Python) -> String {
                crate::utils::PythonRepr::py_repr(self, py)
            }
            fn __str__(&self, py: Python) -> String {
                self.__repr__(py)
            }
            fn __richcmp__(
                &self,
                other: &Self,
                op: CompareOp,
                py: Python<'_>,
            ) -> PyResult<PyObject> {
                let res = match op {
                    CompareOp::Eq => (&self.inner == &other.inner).into_py(py),
                    CompareOp::Ne => (&self.inner != &other.inner).into_py(py),
                    _ => py.NotImplemented(),
                };
                Ok(res)
            }
        }
        impl crate::utils::PythonRepr for $py_class {
            fn py_repr(&self, py: Python) -> String {
                format!(
                    "{}({}, {})",
                    stringify!($py_class),
                    &self.field.clone().into_ref(py).repr().unwrap(),
                    self.output.py_repr(py)
                )
            }
        }
    };
}

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct BooleanEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(BooleanEncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct UInt8EncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(UInt8EncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct UInt16EncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(UInt16EncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct UInt32EncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(UInt32EncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct Int8EncoderBuilder {
    field: Py<PyAny>,
    output: PostgresType,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder_variable_output!(
    Int8EncoderBuilder,
    pgpq::encoders::Int8EncoderBuilder,
    pgpq::encoders::EncoderBuilder::Int8
);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct Int16EncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(Int16EncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct Int32EncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(Int32EncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct Int64EncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(Int64EncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct Float16EncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(Float16EncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct Float32EncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(Float32EncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct Float64EncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(Float64EncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct TimestampMicrosecondEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(TimestampMicrosecondEncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct TimestampMillisecondEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(TimestampMillisecondEncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct TimestampSecondEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(TimestampSecondEncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct Date32EncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(Date32EncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct Time32MillisecondEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(Time32MillisecondEncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct Time32SecondEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(Time32SecondEncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct Time64MicrosecondEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(Time64MicrosecondEncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct DurationMicrosecondEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(DurationMicrosecondEncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct DurationMillisecondEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(DurationMillisecondEncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct DurationSecondEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(DurationSecondEncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct StringEncoderBuilder {
    field: Py<PyAny>,
    output: crate::pg_schema::PostgresType,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder_variable_output!(
    StringEncoderBuilder,
    pgpq::encoders::StringEncoderBuilder,
    pgpq::encoders::EncoderBuilder::String
);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct LargeStringEncoderBuilder {
    field: Py<PyAny>,
    output: crate::pg_schema::PostgresType,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder_variable_output!(
    LargeStringEncoderBuilder,
    pgpq::encoders::LargeStringEncoderBuilder,
    pgpq::encoders::EncoderBuilder::LargeString
);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct BinaryEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(BinaryEncoderBuilder);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct LargeBinaryEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_passthrough_encoder_builder!(LargeBinaryEncoderBuilder);

macro_rules! impl_list {
    ($struct:ident, $encoder_builder_enum_variant:path, $encoder_builder_new_with_inner:expr) => {
        #[pymethods]
        impl $struct {
            #[new]
            fn new(py: Python, py_field: &PyAny) -> PyResult<Self> {
                let field: Field = FromPyArrow::from_pyarrow(py_field)?;
                let inner = match pgpq::encoders::EncoderBuilder::try_new(Arc::new(field)) {
                    Ok(inner) => inner,
                    Err(e) => {
                        return Err(PyValueError::new_err(format!(
                            "Error building {}: {:?}",
                            stringify!($struct),
                            e
                        )));
                    }
                };
                Ok(Self {
                    field: py_field.to_object(py),
                    inner,
                })
            }
            #[classmethod]
            fn new_with_inner(
                _cls: &PyAny,
                py: Python,
                py_field: &PyAny,
                py_inner_encoder_builder: EncoderBuilder,
            ) -> PyResult<Self> {
                let field: Field = FromPyArrow::from_pyarrow(py_field)?;
                let inner_encoder_builder: pgpq::encoders::EncoderBuilder =
                    py_inner_encoder_builder.into();
                Ok(Self {
                    field: py_field.to_object(py),
                    inner: $encoder_builder_enum_variant(
                        $encoder_builder_new_with_inner(Arc::new(field), inner_encoder_builder)
                            .unwrap(),
                    ),
                })
            }
            fn __repr__(&self, py: Python) -> String {
                crate::utils::PythonRepr::py_repr(self, py)
            }
            fn __str__(&self, py: Python) -> String {
                self.__repr__(py)
            }
            fn __richcmp__(
                &self,
                other: &Self,
                op: CompareOp,
                py: Python<'_>,
            ) -> PyResult<PyObject> {
                let res = match op {
                    CompareOp::Eq => (&self.inner == &other.inner).into_py(py),
                    CompareOp::Ne => (&self.inner != &other.inner).into_py(py),
                    _ => py.NotImplemented(),
                };
                Ok(res)
            }
        }
        impl crate::utils::PythonRepr for $struct {
            fn py_repr(&self, py: Python) -> String {
                let inner_encoder_builder = match &self.inner {
                    pgpq::encoders::EncoderBuilder::List(inner) => {
                        EncoderBuilder::from(inner.inner_encoder_builder())
                    }
                    _ => unreachable!(),
                };
                format!(
                    "{}({}, {})",
                    "ListEncoderBuilder",
                    &self.field.clone().into_ref(py).repr().unwrap(),
                    inner_encoder_builder.py_repr(py),
                )
            }
        }
    };
}

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct ListEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_list!(
    ListEncoderBuilder,
    pgpq::encoders::EncoderBuilder::List,
    pgpq::encoders::ListEncoderBuilder::new_with_inner
);

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug, Clone)]
pub struct LargeListEncoderBuilder {
    field: Py<PyAny>,
    inner: pgpq::encoders::EncoderBuilder,
}
impl_list!(
    LargeListEncoderBuilder,
    pgpq::encoders::EncoderBuilder::LargeList,
    pgpq::encoders::LargeListEncoderBuilder::new_with_inner
);

#[derive(FromPyObject, Debug, Clone)]
pub enum EncoderBuilder {
    Boolean(BooleanEncoderBuilder),
    UInt8(UInt8EncoderBuilder),
    UInt16(UInt16EncoderBuilder),
    UInt32(UInt32EncoderBuilder),
    Int8(Int8EncoderBuilder),
    Int16(Int16EncoderBuilder),
    Int32(Int32EncoderBuilder),
    Int64(Int64EncoderBuilder),
    Float16(Float16EncoderBuilder),
    Float32(Float32EncoderBuilder),
    Float64(Float64EncoderBuilder),
    TimestampMicrosecond(TimestampMicrosecondEncoderBuilder),
    TimestampMillisecond(TimestampMillisecondEncoderBuilder),
    TimestampSecond(TimestampSecondEncoderBuilder),
    Date32(Date32EncoderBuilder),
    Time32Millisecond(Time32MillisecondEncoderBuilder),
    Time32Second(Time32SecondEncoderBuilder),
    Time64Microsecond(Time64MicrosecondEncoderBuilder),
    DurationMicrosecond(DurationMicrosecondEncoderBuilder),
    DurationMillisecond(DurationMillisecondEncoderBuilder),
    DurationSecond(DurationSecondEncoderBuilder),
    String(StringEncoderBuilder),
    LargeString(LargeStringEncoderBuilder),
    Binary(BinaryEncoderBuilder),
    LargeBinary(LargeBinaryEncoderBuilder),
    List(ListEncoderBuilder),
    LargeList(LargeListEncoderBuilder),
}

impl crate::utils::PythonRepr for EncoderBuilder {
    fn py_repr(&self, py: Python) -> String {
        match self {
            EncoderBuilder::Boolean(inner) => inner.py_repr(py),
            EncoderBuilder::UInt8(inner) => inner.py_repr(py),
            EncoderBuilder::UInt16(inner) => inner.py_repr(py),
            EncoderBuilder::UInt32(inner) => inner.py_repr(py),
            EncoderBuilder::Int8(inner) => inner.py_repr(py),
            EncoderBuilder::Int16(inner) => inner.py_repr(py),
            EncoderBuilder::Int32(inner) => inner.py_repr(py),
            EncoderBuilder::Int64(inner) => inner.py_repr(py),
            EncoderBuilder::Float16(inner) => inner.py_repr(py),
            EncoderBuilder::Float32(inner) => inner.py_repr(py),
            EncoderBuilder::Float64(inner) => inner.py_repr(py),
            EncoderBuilder::TimestampMicrosecond(inner) => inner.py_repr(py),
            EncoderBuilder::TimestampMillisecond(inner) => inner.py_repr(py),
            EncoderBuilder::TimestampSecond(inner) => inner.py_repr(py),
            EncoderBuilder::Date32(inner) => inner.py_repr(py),
            EncoderBuilder::Time32Millisecond(inner) => inner.py_repr(py),
            EncoderBuilder::Time32Second(inner) => inner.py_repr(py),
            EncoderBuilder::Time64Microsecond(inner) => inner.py_repr(py),
            EncoderBuilder::DurationMicrosecond(inner) => inner.py_repr(py),
            EncoderBuilder::DurationMillisecond(inner) => inner.py_repr(py),
            EncoderBuilder::DurationSecond(inner) => inner.py_repr(py),
            EncoderBuilder::String(inner) => inner.py_repr(py),
            EncoderBuilder::LargeString(inner) => inner.py_repr(py),
            EncoderBuilder::Binary(inner) => inner.py_repr(py),
            EncoderBuilder::LargeBinary(inner) => inner.py_repr(py),
            EncoderBuilder::List(inner) => inner.py_repr(py),
            EncoderBuilder::LargeList(inner) => inner.py_repr(py),
        }
    }
}

impl EncoderBuilder {
    pub fn try_new(py: Python, py_field: &PyAny) -> PyResult<Self> {
        let field: Field = FromPyArrow::from_pyarrow(py_field)?;
        let inner = match pgpq::encoders::EncoderBuilder::try_new(Arc::new(field)) {
            Ok(inner) => inner,
            Err(_e) => {
                return Err(PyRuntimeError::new_err(format!(
                    "Unable to infer encoder for {:?}",
                    py_field.repr().unwrap()
                )))
            }
        };
        let pg_output_type: crate::pg_schema::PostgresType = inner.schema().data_type.into();
        let wrapped = match inner {
            pgpq::encoders::EncoderBuilder::Boolean(_) => {
                EncoderBuilder::Boolean(BooleanEncoderBuilder {
                    field: py_field.to_object(py),
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::UInt8(_) => {
                EncoderBuilder::UInt8(UInt8EncoderBuilder {
                    field: py_field.to_object(py),
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::UInt16(_) => {
                EncoderBuilder::UInt16(UInt16EncoderBuilder {
                    field: py_field.to_object(py),
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::UInt32(_) => {
                EncoderBuilder::UInt32(UInt32EncoderBuilder {
                    field: py_field.to_object(py),
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::Int8(_) => EncoderBuilder::Int8(Int8EncoderBuilder {
                field: py_field.to_object(py),
                output: pg_output_type,
                inner,
            }),
            pgpq::encoders::EncoderBuilder::Int16(_) => {
                EncoderBuilder::Int16(Int16EncoderBuilder {
                    field: py_field.to_object(py),
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::Int32(_) => {
                EncoderBuilder::Int32(Int32EncoderBuilder {
                    field: py_field.to_object(py),
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::Int64(_) => {
                EncoderBuilder::Int64(Int64EncoderBuilder {
                    field: py_field.to_object(py),
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::Float16(_) => {
                EncoderBuilder::Float16(Float16EncoderBuilder {
                    field: py_field.to_object(py),
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::Float32(_) => {
                EncoderBuilder::Float32(Float32EncoderBuilder {
                    field: py_field.to_object(py),
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::Float64(_) => {
                EncoderBuilder::Float64(Float64EncoderBuilder {
                    field: py_field.to_object(py),
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::TimestampMicrosecond(_) => {
                EncoderBuilder::TimestampMicrosecond(TimestampMicrosecondEncoderBuilder {
                    field: py_field.to_object(py),
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::TimestampMillisecond(_) => {
                EncoderBuilder::TimestampMillisecond(TimestampMillisecondEncoderBuilder {
                    field: py_field.to_object(py),
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::TimestampSecond(_) => {
                EncoderBuilder::TimestampSecond(TimestampSecondEncoderBuilder {
                    field: py_field.to_object(py),
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::Date32(_) => {
                EncoderBuilder::Date32(Date32EncoderBuilder {
                    field: py_field.to_object(py),
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::Time32Millisecond(_) => {
                EncoderBuilder::Time32Millisecond(Time32MillisecondEncoderBuilder {
                    field: py_field.to_object(py),
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::Time32Second(_) => {
                EncoderBuilder::Time32Second(Time32SecondEncoderBuilder {
                    field: py_field.to_object(py),
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::Time64Microsecond(_) => {
                EncoderBuilder::Time64Microsecond(Time64MicrosecondEncoderBuilder {
                    field: py_field.to_object(py),
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::DurationMicrosecond(_) => {
                EncoderBuilder::DurationMicrosecond(DurationMicrosecondEncoderBuilder {
                    field: py_field.to_object(py),
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::DurationMillisecond(_) => {
                EncoderBuilder::DurationMillisecond(DurationMillisecondEncoderBuilder {
                    field: py_field.to_object(py),
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::DurationSecond(_) => {
                EncoderBuilder::DurationSecond(DurationSecondEncoderBuilder {
                    field: py_field.to_object(py),
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::String(_) => {
                EncoderBuilder::String(StringEncoderBuilder {
                    field: py_field.to_object(py),
                    output: pg_output_type,
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::LargeString(_) => {
                EncoderBuilder::LargeString(LargeStringEncoderBuilder {
                    field: py_field.to_object(py),
                    output: pg_output_type,
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::Binary(_) => {
                EncoderBuilder::Binary(BinaryEncoderBuilder {
                    field: py_field.to_object(py),
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::LargeBinary(_) => {
                EncoderBuilder::LargeBinary(LargeBinaryEncoderBuilder {
                    field: py_field.to_object(py),
                    inner,
                })
            }
            pgpq::encoders::EncoderBuilder::List(_) => EncoderBuilder::List(ListEncoderBuilder {
                field: py_field.to_object(py),
                inner,
            }),
            pgpq::encoders::EncoderBuilder::LargeList(_) => {
                EncoderBuilder::LargeList(LargeListEncoderBuilder {
                    field: py_field.to_object(py),
                    inner,
                })
            }
        };
        Ok(wrapped)
    }
}

impl From<pgpq::encoders::EncoderBuilder> for EncoderBuilder {
    fn from(value: pgpq::encoders::EncoderBuilder) -> Self {
        Python::with_gil(|py| match &value {
            pgpq::encoders::EncoderBuilder::Boolean(inner) => {
                let field = inner.field();
                EncoderBuilder::Boolean(BooleanEncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                })
            }
            pgpq::encoders::EncoderBuilder::UInt8(inner) => {
                let field = inner.field();
                EncoderBuilder::UInt8(UInt8EncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                })
            }
            pgpq::encoders::EncoderBuilder::UInt16(inner) => {
                let field = inner.field();
                EncoderBuilder::UInt16(UInt16EncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                })
            }
            pgpq::encoders::EncoderBuilder::UInt32(inner) => {
                let field = inner.field();
                EncoderBuilder::UInt32(UInt32EncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                })
            }
            pgpq::encoders::EncoderBuilder::Int8(inner) => {
                let field = inner.field();
                let output: crate::pg_schema::PostgresType = inner.schema().data_type.into();
                EncoderBuilder::Int8(Int8EncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                    output,
                })
            }
            pgpq::encoders::EncoderBuilder::Int16(inner) => {
                let field = inner.field();
                EncoderBuilder::Int16(Int16EncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                })
            }
            pgpq::encoders::EncoderBuilder::Int32(inner) => {
                let field = inner.field();
                EncoderBuilder::Int32(Int32EncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                })
            }
            pgpq::encoders::EncoderBuilder::Int64(inner) => {
                let field = inner.field();
                EncoderBuilder::Int64(Int64EncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                })
            }
            pgpq::encoders::EncoderBuilder::Float16(inner) => {
                let field = inner.field();
                EncoderBuilder::Float16(Float16EncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                })
            }
            pgpq::encoders::EncoderBuilder::Float32(inner) => {
                let field = inner.field();
                EncoderBuilder::Float32(Float32EncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                })
            }
            pgpq::encoders::EncoderBuilder::Float64(inner) => {
                let field = inner.field();
                EncoderBuilder::Float64(Float64EncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                })
            }
            pgpq::encoders::EncoderBuilder::TimestampMicrosecond(inner) => {
                let field = inner.field();
                EncoderBuilder::TimestampMicrosecond(TimestampMicrosecondEncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                })
            }
            pgpq::encoders::EncoderBuilder::TimestampMillisecond(inner) => {
                let field = inner.field();
                EncoderBuilder::TimestampMillisecond(TimestampMillisecondEncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                })
            }
            pgpq::encoders::EncoderBuilder::TimestampSecond(inner) => {
                let field = inner.field();
                EncoderBuilder::TimestampSecond(TimestampSecondEncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                })
            }
            pgpq::encoders::EncoderBuilder::Date32(inner) => {
                let field = inner.field();
                EncoderBuilder::Date32(Date32EncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                })
            }
            pgpq::encoders::EncoderBuilder::Time32Millisecond(inner) => {
                let field = inner.field();
                EncoderBuilder::Time32Millisecond(Time32MillisecondEncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                })
            }
            pgpq::encoders::EncoderBuilder::Time32Second(inner) => {
                let field = inner.field();
                EncoderBuilder::Time32Second(Time32SecondEncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                })
            }
            pgpq::encoders::EncoderBuilder::Time64Microsecond(inner) => {
                let field = inner.field();
                EncoderBuilder::Time64Microsecond(Time64MicrosecondEncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                })
            }
            pgpq::encoders::EncoderBuilder::DurationMicrosecond(inner) => {
                let field = inner.field();
                EncoderBuilder::DurationMicrosecond(DurationMicrosecondEncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                })
            }
            pgpq::encoders::EncoderBuilder::DurationMillisecond(inner) => {
                let field = inner.field();
                EncoderBuilder::DurationMillisecond(DurationMillisecondEncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                })
            }
            pgpq::encoders::EncoderBuilder::DurationSecond(inner) => {
                let field = inner.field();
                EncoderBuilder::DurationSecond(DurationSecondEncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                })
            }
            pgpq::encoders::EncoderBuilder::String(inner) => {
                let field = inner.field();
                let output: crate::pg_schema::PostgresType = inner.schema().data_type.into();
                EncoderBuilder::String(StringEncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                    output,
                })
            }
            pgpq::encoders::EncoderBuilder::LargeString(inner) => {
                let field = inner.field();
                let output: crate::pg_schema::PostgresType = inner.schema().data_type.into();
                EncoderBuilder::LargeString(LargeStringEncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                    output,
                })
            }
            pgpq::encoders::EncoderBuilder::Binary(inner) => {
                let field = inner.field();
                EncoderBuilder::Binary(BinaryEncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                })
            }
            pgpq::encoders::EncoderBuilder::LargeBinary(inner) => {
                let field = inner.field();
                EncoderBuilder::LargeBinary(LargeBinaryEncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                })
            }
            pgpq::encoders::EncoderBuilder::List(inner) => {
                let field = inner.field();
                EncoderBuilder::List(ListEncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                })
            }
            pgpq::encoders::EncoderBuilder::LargeList(inner) => {
                let field = inner.field();
                EncoderBuilder::LargeList(LargeListEncoderBuilder {
                    field: field.to_pyarrow(py).unwrap(),
                    inner: value,
                })
            }
        })
    }
}

impl From<EncoderBuilder> for pgpq::encoders::EncoderBuilder {
    fn from(val: EncoderBuilder) -> Self {
        match val {
            EncoderBuilder::Boolean(inner) => inner.inner,
            EncoderBuilder::UInt8(inner) => inner.inner,
            EncoderBuilder::UInt16(inner) => inner.inner,
            EncoderBuilder::UInt32(inner) => inner.inner,
            EncoderBuilder::Int8(inner) => inner.inner,
            EncoderBuilder::Int16(inner) => inner.inner,
            EncoderBuilder::Int32(inner) => inner.inner,
            EncoderBuilder::Int64(inner) => inner.inner,
            EncoderBuilder::Float16(inner) => inner.inner,
            EncoderBuilder::Float32(inner) => inner.inner,
            EncoderBuilder::Float64(inner) => inner.inner,
            EncoderBuilder::TimestampMicrosecond(inner) => inner.inner,
            EncoderBuilder::TimestampMillisecond(inner) => inner.inner,
            EncoderBuilder::TimestampSecond(inner) => inner.inner,
            EncoderBuilder::Date32(inner) => inner.inner,
            EncoderBuilder::Time32Millisecond(inner) => inner.inner,
            EncoderBuilder::Time32Second(inner) => inner.inner,
            EncoderBuilder::Time64Microsecond(inner) => inner.inner,
            EncoderBuilder::DurationMicrosecond(inner) => inner.inner,
            EncoderBuilder::DurationMillisecond(inner) => inner.inner,
            EncoderBuilder::DurationSecond(inner) => inner.inner,
            EncoderBuilder::String(inner) => inner.inner,
            EncoderBuilder::LargeString(inner) => inner.inner,
            EncoderBuilder::Binary(inner) => inner.inner,
            EncoderBuilder::LargeBinary(inner) => inner.inner,
            EncoderBuilder::List(inner) => inner.inner,
            EncoderBuilder::LargeList(inner) => inner.inner,
        }
    }
}

impl IntoPy<PyObject> for EncoderBuilder {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self {
            EncoderBuilder::Boolean(inner) => inner.into_py(py),
            EncoderBuilder::UInt8(inner) => inner.into_py(py),
            EncoderBuilder::UInt16(inner) => inner.into_py(py),
            EncoderBuilder::UInt32(inner) => inner.into_py(py),
            EncoderBuilder::Int8(inner) => inner.into_py(py),
            EncoderBuilder::Int16(inner) => inner.into_py(py),
            EncoderBuilder::Int32(inner) => inner.into_py(py),
            EncoderBuilder::Int64(inner) => inner.into_py(py),
            EncoderBuilder::Float16(inner) => inner.into_py(py),
            EncoderBuilder::Float32(inner) => inner.into_py(py),
            EncoderBuilder::Float64(inner) => inner.into_py(py),
            EncoderBuilder::TimestampMicrosecond(inner) => inner.into_py(py),
            EncoderBuilder::TimestampMillisecond(inner) => inner.into_py(py),
            EncoderBuilder::TimestampSecond(inner) => inner.into_py(py),
            EncoderBuilder::Date32(inner) => inner.into_py(py),
            EncoderBuilder::Time32Millisecond(inner) => inner.into_py(py),
            EncoderBuilder::Time32Second(inner) => inner.into_py(py),
            EncoderBuilder::Time64Microsecond(inner) => inner.into_py(py),
            EncoderBuilder::DurationMicrosecond(inner) => inner.into_py(py),
            EncoderBuilder::DurationMillisecond(inner) => inner.into_py(py),
            EncoderBuilder::DurationSecond(inner) => inner.into_py(py),
            EncoderBuilder::String(inner) => inner.into_py(py),
            EncoderBuilder::LargeString(inner) => inner.into_py(py),
            EncoderBuilder::Binary(inner) => inner.into_py(py),
            EncoderBuilder::LargeBinary(inner) => inner.into_py(py),
            EncoderBuilder::List(inner) => inner.into_py(py),
            EncoderBuilder::LargeList(inner) => inner.into_py(py),
        }
    }
}
