use std::error;
use std::fmt;
use std::io;

#[derive(Debug, PartialEq)]
enum Kind {
    ToSql(String),
    Encode,
}

#[derive(Debug)]
struct ErrorInner {
    kind: Kind,
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
            Kind::ToSql(idx) => write!(fmt, "error serializing parameter {idx}")?,
            Kind::Encode => write!(fmt, "error encoding message")?,
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
    fn new(kind: Kind, cause: Option<Box<dyn error::Error + Sync + Send>>) -> Error {
        Error(Box::new(ErrorInner { kind, cause }))
    }

    /// Consumes the error, returning its cause.
    pub fn into_source(self) -> Option<Box<dyn error::Error + Sync + Send>> {
        self.0.cause
    }

    pub(crate) fn encode(e: io::Error) -> Error {
        Error::new(Kind::Encode, Some(Box::new(e)))
    }

    #[allow(clippy::wrong_self_convention)]
    pub(crate) fn to_sql(e: Box<dyn error::Error + Sync + Send>, col: &str) -> Error {
        Error::new(Kind::ToSql(col.to_string()), Some(e))
    }
}
