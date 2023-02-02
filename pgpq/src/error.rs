use arrow_schema::DataType;
use std::error;
use std::fmt;

use crate::PostgresField;

#[derive(Debug, PartialEq)]
pub enum ErrorKind {
    TypeNotSupported { field: String, tp: DataType },
    FieldTooLarge { field: PostgresField, size: usize }, // Postgres' binary format only supports fields up to 32bits
    ToSql { field: PostgresField },
    Encode,
}

#[derive(Debug)]
struct ErrorInner {
    kind: ErrorKind,
    cause: Option<Box<dyn error::Error + Sync + Send>>,
}

/// An error communicating with the Postgres server.
pub struct Error(Box<ErrorInner>);

impl fmt::Debug for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Error")
            .field("kind", &self.0.kind)
            .field("cause", &self.0.cause)
            .finish()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0.kind {
            ErrorKind::ToSql { field } => {
                write!(fmt, "error serializing parameter {}", field.name)?
            }
            ErrorKind::Encode => write!(fmt, "error encoding message")?,
            ErrorKind::FieldTooLarge { field, size } => write!(
                fmt,
                "field {} exceeds the maximum allowed size for binary copy ({} bytes)",
                field.name, size
            )?,
            ErrorKind::TypeNotSupported { field, tp } => {
                write!(fmt, "Arrow type {tp} for field {field} is not supported")?
            }
        };
        if let Some(ref cause) = self.0.cause {
            write!(fmt, ": {cause}")?;
        }
        Ok(())
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        self.0.cause.as_ref().map(|e| &**e as _)
    }
}

impl Error {
    pub fn new(kind: ErrorKind, cause: Option<Box<dyn error::Error + Sync + Send>>) -> Error {
        Error(Box::new(ErrorInner { kind, cause }))
    }

    #[allow(clippy::wrong_self_convention)]
    pub(crate) fn to_sql(e: Box<dyn error::Error + Sync + Send>, field: &PostgresField) -> Error {
        Error::new(
            ErrorKind::ToSql {
                field: field.clone(),
            },
            Some(e),
        )
    }

    pub(crate) fn field_too_large(field: &PostgresField, size: usize) -> Error {
        Error::new(
            ErrorKind::FieldTooLarge {
                field: field.clone(),
                size,
            },
            None,
        )
    }

    pub(crate) fn type_unsupported(field: &str, tp: &DataType) -> Error {
        Error::new(
            ErrorKind::TypeNotSupported {
                field: field.to_string(),
                tp: tp.clone(),
            },
            None,
        )
    }
}
