//! A single embedded Postgres instance shared by every case in a test.
//!
//! Starting Postgres is by far the most expensive part of these tests, so a [`TestDb`] is started
//! once per test function and every case is run against it inside a transaction that is rolled
//! back afterwards. That keeps cases independent without paying for a new server (or even a new
//! database) each time.

use std::collections::HashMap;
use std::error::Error;
use std::io::Write;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use bytes::BytesMut;
use pgpq::encoders::EncoderBuilder;
use pgpq::pg_schema::PostgresSchema;
use pgpq::ArrowToPostgresBinaryEncoder;
use postgres::{Client, NoTls};
use postgresql_embedded::blocking::PostgreSQL;
use postgresql_embedded::Settings;

use super::value::Value;

type BoxError = Box<dyn Error>;

/// Encode `batches` into Postgres' binary COPY format, returning the buffer and the Postgres
/// schema the encoder derived from `schema`.
pub fn encode(
    schema: &Schema,
    batches: &[RecordBatch],
    encoders: Option<&HashMap<String, EncoderBuilder>>,
) -> Result<(BytesMut, PostgresSchema), BoxError> {
    let mut encoder = match encoders {
        Some(encoders) => ArrowToPostgresBinaryEncoder::try_new_with_encoders(schema, encoders)?,
        None => ArrowToPostgresBinaryEncoder::try_new(schema)?,
    };
    let pg_schema = encoder.schema();
    let mut buf = BytesMut::new();
    encoder.write_header(&mut buf);
    for batch in batches {
        encoder.write_batch(batch, &mut buf)?;
    }
    encoder.write_footer(&mut buf)?;
    Ok((buf, pg_schema))
}

pub struct TestDb {
    // Declared before `postgres` so that the client is disconnected before the server is stopped.
    client: Client,
    #[allow(dead_code)]
    postgres: PostgreSQL,
}

impl TestDb {
    pub fn start() -> Result<TestDb, BoxError> {
        let settings = Settings {
            timeout: Some(Duration::from_secs(60)),
            ..Default::default()
        };
        let mut postgres = PostgreSQL::new(settings);
        postgres.setup()?;
        postgres.start()?;
        postgres.create_database("test")?;
        let settings = postgres.settings();
        let client = Client::connect(
            &format!(
                "host=localhost port={} user={} password={} dbname=test",
                settings.port, settings.username, settings.password
            ),
            NoTls,
        )?;
        Ok(TestDb { client, postgres })
    }

    pub fn client(&mut self) -> &mut Client {
        &mut self.client
    }

    /// Create a table from `pg_schema`, `COPY` `buf` into it and read every row back with typed
    /// `FromSql` decoding. The table (and any composite types it needs) only exist for the
    /// duration of the call.
    pub fn load_and_read(
        &mut self,
        table: &str,
        pg_schema: &PostgresSchema,
        buf: &[u8],
    ) -> Result<Vec<Vec<Value>>, BoxError> {
        let mut tx = self.client.transaction()?;
        tx.batch_execute(&pg_schema.ddl(table, false))?;
        let mut writer = tx.copy_in(format!("copy \"{table}\" from stdin binary").as_str())?;
        writer.write_all(buf)?;
        writer.finish()?;
        let rows = tx.query(
            format!("select * from \"{table}\" order by ctid").as_str(),
            &[],
        )?;
        let mut decoded = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut values = Vec::with_capacity(row.len());
            for i in 0..row.len() {
                values.push(row.try_get::<_, Value>(i)?);
            }
            decoded.push(values);
        }
        // Rolling back drops the table and any composite types created above.
        tx.rollback()?;
        Ok(decoded)
    }

    /// Encode `batches`, push them through Postgres and read them back.
    pub fn roundtrip(
        &mut self,
        table: &str,
        schema: &Schema,
        batches: &[RecordBatch],
        encoders: Option<&HashMap<String, EncoderBuilder>>,
    ) -> Result<Vec<Vec<Value>>, BoxError> {
        let (buf, pg_schema) = encode(schema, batches, encoders)?;
        self.load_and_read(table, &pg_schema, &buf)
    }
}
