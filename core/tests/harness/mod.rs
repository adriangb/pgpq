//! Shared machinery for the Postgres backed integration tests.
//!
//! * [`value`] defines a normalized value type that can be produced both from an Arrow array and
//!   from a Postgres row (via a typed `FromSql` decode of the binary wire format).
//! * [`db`] owns a single embedded Postgres instance and runs cases against it.
//! * [`cases`] enumerates the roundtrip cases.
//!
//! The pieces are deliberately independent of *how* the Arrow data is produced so that a property
//! based test can build [`cases::Case`]s on the fly and reuse the same assertions.
#![allow(dead_code)]

pub mod cases;
pub mod db;
pub mod value;
