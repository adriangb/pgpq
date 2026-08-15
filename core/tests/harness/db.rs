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
use pgpq::ArrowToPostgresBinaryEncoder;
use pgpq::encoders::EncoderBuilder;
use pgpq::pg_schema::{Column, PostgresSchema, PostgresType};
use postgres::{Client, NoTls};
use postgresql_embedded::Settings;
use postgresql_embedded::blocking::PostgreSQL;

use super::value::Value;

type BoxError = Box<dyn Error>;

/// Encode `batches` into Postgres' binary COPY format, returning the buffer and the Postgres
/// schema the encoder derived from `schema`.
pub fn encode(
    schema: &Schema,
    batches: &[RecordBatch],
    encoders: Option<&HashMap<String, EncoderBuilder>>,
) -> Result<(BytesMut, PostgresSchema), BoxError> {
    let encoder = build_encoder(schema, encoders)?;
    let pg_schema = encoder.schema();
    let oids = composite_type_names(&pg_schema)
        .into_iter()
        // No server here to ask, so the historical placeholder stands in; see
        // `corpus_composite_oids` in integration_tests.rs.
        .map(|name| (name, 16_385))
        .collect::<HashMap<_, _>>();
    let encoder = if oids.is_empty() {
        encoder
    } else {
        encoder.with_composite_oids(&oids)?
    };
    Ok((encode_batches(encoder, batches)?, pg_schema))
}

fn build_encoder(
    schema: &Schema,
    encoders: Option<&HashMap<String, EncoderBuilder>>,
) -> Result<ArrowToPostgresBinaryEncoder, BoxError> {
    Ok(match encoders {
        Some(encoders) => ArrowToPostgresBinaryEncoder::try_new_with_encoders(schema, encoders)?,
        None => ArrowToPostgresBinaryEncoder::try_new(schema)?,
    })
}

fn encode_batches(
    mut encoder: ArrowToPostgresBinaryEncoder,
    batches: &[RecordBatch],
) -> Result<BytesMut, BoxError> {
    let mut buf = BytesMut::new();
    encoder.write_header(&mut buf)?;
    for batch in batches {
        encoder.write_batch(batch, &mut buf)?;
    }
    encoder.write_footer(&mut buf)?;
    Ok(buf)
}

/// Every composite type name the generated DDL will create, outermost first.
pub fn composite_type_names(pg_schema: &PostgresSchema) -> Vec<String> {
    fn walk(column: &Column, out: &mut Vec<String>) {
        match &column.data_type {
            PostgresType::UserDefined { fields, .. } => {
                out.push(format!("{}_t", column.name));
                for field in fields {
                    walk(field, out);
                }
            }
            PostgresType::List(inner) => walk(inner, out),
            _ => {}
        }
    }

    let mut out = Vec::new();
    for column in &pg_schema.columns {
        walk(column, &mut out);
    }
    out
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

    /// Create the table (and any composite types) for `pg_schema` and `COPY` `buf` into it,
    /// rolling back afterwards. Used to check that a *particular* encoding is accepted or
    /// rejected by the server, rather than to read values back.
    pub fn load_snapshot(
        &mut self,
        table: &str,
        pg_schema: &PostgresSchema,
        buf: &[u8],
    ) -> Result<(), BoxError> {
        let mut tx = self.client.transaction()?;
        tx.batch_execute(&pg_schema.ddl(table, false))?;
        let mut writer = tx.copy_in(format!("copy \"{table}\" from stdin binary").as_str())?;
        writer.write_all(buf)?;
        writer.finish()?;
        tx.rollback()?;
        Ok(())
    }

    /// Encode `batches`, push them through Postgres and read them back.
    pub fn roundtrip(
        &mut self,
        table: &str,
        schema: &Schema,
        batches: &[RecordBatch],
        encoders: Option<&HashMap<String, EncoderBuilder>>,
    ) -> Result<Vec<Vec<Value>>, BoxError> {
        let encoder = build_encoder(schema, encoders)?;
        let pg_schema = encoder.schema();

        let mut tx = self.client.transaction()?;
        tx.batch_execute(&pg_schema.ddl(table, false))?;

        // The composite types now exist, so ask the server what OIDs it gave them and encode
        // with those. This is the flow `with_composite_oids` is for, and it means the roundtrip
        // never depends on a composite landing on a particular OID (#96).
        let mut oids = HashMap::new();
        for name in composite_type_names(&pg_schema) {
            let oid: u32 = tx
                .query_one("select oid from pg_type where typname = $1", &[&name])?
                .get(0);
            oids.insert(name, oid);
        }
        let encoder = if oids.is_empty() {
            encoder
        } else {
            encoder.with_composite_oids(&oids)?
        };
        let buf = encode_batches(encoder, batches)?;

        let mut writer = tx.copy_in(format!("copy \"{table}\" from stdin binary").as_str())?;
        writer.write_all(&buf)?;
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
}
