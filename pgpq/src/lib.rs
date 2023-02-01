mod error;

use std::convert::TryFrom;
use std::io;
use std::iter::zip;

use crate::error::Error;
use arrow::array;
use arrow::array::as_primitive_array;
use arrow::datatypes::{DataType, Schema, TimeUnit};

use arrow::record_batch::RecordBatch;

use arrow_array::types as array_types;

use byteorder::{BigEndian, ByteOrder};
use bytes::{BufMut, Bytes, BytesMut};
use chrono::{TimeZone, Utc};

use postgres_types::{to_sql_checked, IsNull, ToSql, Type};

const MAGIC: &[u8] = b"PGCOPY\n\xff\r\n\0";

fn write_null(buf: &mut BytesMut) {
    let idx = buf.len();
    buf.put_i32(0);
    BigEndian::write_i32(&mut buf[idx..], -1);
}

fn write_value(
    v: &dyn ToSql,
    type_: &Type,
    field_name: &str,
    buf: &mut BytesMut,
) -> Result<(), Error> {
    let idx = buf.len();
    buf.put_i32(0);
    let len = match v
        .to_sql_checked(type_, buf)
        .map_err(|e| Error::to_sql(e, field_name))?
    {
        IsNull::Yes => -1,
        IsNull::No => i32::try_from(buf.len() - idx - 4)
            .map_err(|e| Error::encode(io::Error::new(io::ErrorKind::InvalidInput, e)))?,
    };
    BigEndian::write_i32(&mut buf[idx..], len);
    Ok(())
}

fn arrow_type_to_pg_type(tp: &DataType) -> Type {
    match tp {
        DataType::Null => {
            // Any value will do
            Type::INT2
        }
        DataType::Boolean => Type::BOOL,
        DataType::Int8 => Type::INT2,
        DataType::Int16 => Type::INT2,
        DataType::Int32 => Type::INT4,
        DataType::Int64 => Type::INT8,
        DataType::UInt8 => Type::INT2,
        DataType::UInt16 => Type::INT4,
        DataType::UInt32 => Type::INT8,
        DataType::Float16 => Type::FLOAT4,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Timestamp(_, None) => Type::TIMESTAMP,
        DataType::Timestamp(_, Some(_)) => Type::TIMESTAMPTZ,
        DataType::Date32 => Type::DATE,
        DataType::Date64 => Type::DATE,
        DataType::Time32(_) => Type::TIME,
        DataType::Duration(_) => Type::INTERVAL,
        DataType::Binary => Type::BYTEA,
        DataType::FixedSizeBinary(_) => Type::BYTEA,
        DataType::LargeBinary => Type::BYTEA,
        DataType::Utf8 => Type::TEXT,
        DataType::LargeUtf8 => Type::TEXT,
        DataType::List(_) => {
            panic!("TODO, needs a lot of boilerplate")
        }
        DataType::FixedSizeList(_, _) => {
            panic!("TODO, needs a lot of boilerplate")
        }
        DataType::LargeList(_) => {
            panic!("TODO, needs a lot of boilerplate")
        }
        DataType::Struct(_) => {
            panic!("TODO, unpack as JSONB")
        }
        DataType::Dictionary(_, _) => {
            panic!("TODO, unpack as JSONB")
        }
        DataType::Decimal128(_, _) => {
            panic!("TODO, use https://docs.rs/rust_decimal/latest/rust_decimal/prelude/struct.Decimal.html#method.to_sql")
        }
        DataType::Decimal256(_, _) => {
            panic!("TODO, use https://docs.rs/rust_decimal/latest/rust_decimal/prelude/struct.Decimal.html#method.to_sql")
        }
        _ => panic!("UNSUPPORTED"),
    }
}

#[derive(Debug, Clone)]
pub struct PostgresField {
    type_: Type,
    name: String,
}

#[derive(Debug)]
pub struct ArrowToPostgresBinaryEncoder {
    fields: Vec<PostgresField>,
}

enum TimestampArray<'a> {
    Nanosecond(&'a array::TimestampNanosecondArray),
    Microsecond(&'a array::TimestampMicrosecondArray),
    Millisecond(&'a array::TimestampMillisecondArray),
    Second(&'a array::TimestampSecondArray),
}

impl<'a> TimestampArray<'a> {
    pub fn value_as_datetime(&self, i: usize) -> Option<chrono::NaiveDateTime> {
        match self {
            Self::Nanosecond(arr) => arr.value_as_datetime(i),
            Self::Microsecond(arr) => arr.value_as_datetime(i),
            Self::Millisecond(arr) => arr.value_as_datetime(i),
            Self::Second(arr) => arr.value_as_datetime(i),
        }
    }
}

enum Time32Array<'a> {
    Millisecond(&'a array::Time32MillisecondArray),
    Second(&'a array::Time32SecondArray),
}

impl<'a> Time32Array<'a> {
    pub fn value_as_time(&self, i: usize) -> Option<chrono::NaiveTime> {
        match self {
            Self::Millisecond(arr) => arr.value_as_time(i),
            Self::Second(arr) => arr.value_as_time(i),
        }
    }
}

enum Time64Array<'a> {
    Nanosecond(&'a array::Time64NanosecondArray),
    Microsecond(&'a array::Time64MicrosecondArray),
}

impl<'a> Time64Array<'a> {
    pub fn value_as_time(&self, i: usize) -> Option<chrono::NaiveTime> {
        match self {
            Self::Nanosecond(arr) => arr.value_as_time(i),
            Self::Microsecond(arr) => arr.value_as_time(i),
        }
    }
}

#[derive(Debug)]
struct PostgresDuration {
    microseconds: i64,
}

impl ToSql for PostgresDuration {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<(dyn std::error::Error + Send + Sync)>> {
        out.put_i64(self.microseconds);
        Ok(IsNull::No)
    }
    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::INTERVAL)
    }

    to_sql_checked!();
}

fn get_1d_index_from_row_col(row: usize, col: usize, n_rows: usize) -> usize {
    // arranged as [col1_row1, col1_row2, col2_row1, col2_row2]
    // to access col2_row1 we do 2 * (2 - 1) + 1 - 1 = 2
    // to access col2_row2 we do 2 * (2 - 1) + 2 - 1= 3
    col * n_rows + row
}

impl ArrowToPostgresBinaryEncoder {
    /// Creates a new writer which will write rows of the provided types to the provided sink.
    pub fn new(schema: Schema) -> ArrowToPostgresBinaryEncoder {
        let pg_fields: Vec<PostgresField> = schema
            .fields
            .iter()
            .map(|f| PostgresField {
                type_: arrow_type_to_pg_type(f.data_type()),
                name: f.name().to_string(),
            })
            .collect();

        ArrowToPostgresBinaryEncoder {
            fields: pg_fields,
        }
    }

    pub fn encode(&mut self, batch: RecordBatch) -> Result<Bytes, Error> {
        assert!(
            batch.num_columns() == self.fields.len(),
            "expected {} values but got {}",
            self.fields.len(),
            batch.num_columns(),
        );
        let n_rows = batch.num_rows();
        let n_cols = batch.num_columns();

        let columns = batch.columns();
        let mut buf = BytesMut::new();
        let mut encoded: Vec<Bytes> = Vec::with_capacity(n_rows * n_cols);
        let fields = self.fields.clone();
        for (arr_ref, field) in zip(columns.iter(), fields) {
            let arr = &*arr_ref.clone();
            let field_name = &field.name;
            let pg_type = &field.type_;
            let mut write = |v: &dyn ToSql| {
                assert!(buf.is_empty());
                write_value(v, pg_type, field_name, &mut buf).unwrap();
                encoded.push(buf.split().freeze());
            };
            match arr.data_type() {
                DataType::Null => {
                    for _ in 0..n_rows {
                        write_null(&mut buf)
                    }
                }
                DataType::Boolean => {
                    let arr = array::as_boolean_array(arr);
                    for v in arr.iter() {
                        write(&v)
                    }
                }
                DataType::Int8 => {
                    let arr = as_primitive_array::<array_types::Int8Type>(arr);
                    for v in arr.iter() {
                        write(&v.map(i16::from))
                    }
                }
                DataType::Int16 => {
                    let arr = as_primitive_array::<array_types::Int16Type>(arr);
                    for v in arr.iter() {
                        write(&v)
                    }
                }
                DataType::Int32 => {
                    let arr = as_primitive_array::<array_types::Int32Type>(arr);
                    for v in arr.iter() {
                        write(&v)
                    }
                }
                DataType::Int64 => {
                    let arr = as_primitive_array::<array_types::Int64Type>(arr);
                    for v in arr.iter() {
                        write(&v)
                    }
                }
                DataType::UInt8 => {
                    let arr = as_primitive_array::<array_types::UInt8Type>(arr);
                    for v in arr.iter() {
                        write(&v.map(i16::from))
                    }
                }
                DataType::UInt16 => {
                    let arr = as_primitive_array::<array_types::UInt16Type>(arr);
                    for v in arr.iter() {
                        write(&v.map(i32::from))
                    }
                }
                DataType::UInt32 => {
                    let arr = as_primitive_array::<array_types::UInt32Type>(arr);
                    for v in arr.iter() {
                        write(&v.map(i64::from))
                    }
                }
                DataType::UInt64 => {
                    panic!("rust-postgres doesn't support u64 or i128")
                }
                DataType::Float16 => {
                    let arr = as_primitive_array::<array_types::Float16Type>(arr);
                    for v in arr.iter() {
                        write(&v.map(f32::from))
                    }
                }
                DataType::Float32 => {
                    let arr = as_primitive_array::<array_types::Float32Type>(arr);
                    for v in arr.iter() {
                        write(&v)
                    }
                }
                DataType::Float64 => {
                    let arr = as_primitive_array::<array_types::Float64Type>(arr);
                    for v in arr.iter() {
                        write(&v)
                    }
                }
                DataType::Timestamp(time_unit, tz_option) => {
                    let arr = match time_unit {
                        TimeUnit::Nanosecond => {
                            TimestampArray::Nanosecond(as_primitive_array::<
                                array_types::TimestampNanosecondType,
                            >(arr))
                        }
                        TimeUnit::Microsecond => {
                            TimestampArray::Microsecond(as_primitive_array::<
                                array_types::TimestampMicrosecondType,
                            >(arr))
                        }
                        TimeUnit::Millisecond => {
                            TimestampArray::Millisecond(as_primitive_array::<
                                array_types::TimestampMillisecondType,
                            >(arr))
                        }
                        TimeUnit::Second => TimestampArray::Second(as_primitive_array::<
                            array_types::TimestampSecondType,
                        >(arr)),
                    };
                    match tz_option {
                        Some(_) => {
                            for row in 0..n_rows {
                                let v = arr
                                    .value_as_datetime(row)
                                    .map(|v| Utc.from_local_datetime(&v).unwrap());
                                write(&v)
                            }
                        }
                        None => {
                            for row in 0..n_rows {
                                let v = arr.value_as_datetime(row);
                                write(&v)
                            }
                        }
                    }
                }
                DataType::Date32 => {
                    let arr = as_primitive_array::<array_types::Date32Type>(arr);
                    for v in arr.iter() {
                        write(&v)
                    }
                }
                DataType::Date64 => {
                    let arr = as_primitive_array::<array_types::Date64Type>(arr);
                    for v in arr.iter() {
                        write(&v)
                    }
                }
                DataType::Time32(time_unit) => {
                    let arr = match time_unit {
                        TimeUnit::Millisecond => {
                            Time32Array::Millisecond(as_primitive_array::<
                                array_types::Time32MillisecondType,
                            >(arr))
                        }
                        TimeUnit::Second => Time32Array::Second(as_primitive_array::<
                            array_types::Time32SecondType,
                        >(arr)),
                        _ => panic!("Time32 does not support {time_unit:?}"),
                    };
                    for row in 0..n_rows {
                        let v = arr.value_as_time(row);
                        write(&v)
                    }
                }
                DataType::Time64(time_unit) => {
                    let arr = match time_unit {
                        TimeUnit::Nanosecond => {
                            Time64Array::Nanosecond(as_primitive_array::<
                                array_types::Time64NanosecondType,
                            >(arr))
                        }
                        TimeUnit::Microsecond => {
                            Time64Array::Microsecond(as_primitive_array::<
                                array_types::Time64MicrosecondType,
                            >(arr))
                        }
                        _ => panic!("Time64 does not support {time_unit:?}"),
                    };
                    for row in 0..n_rows {
                        let v = arr.value_as_time(row);
                        write(&v)
                    }
                }
                DataType::Duration(time_unit) => match time_unit {
                    TimeUnit::Microsecond => {
                        let arr = as_primitive_array::<array_types::DurationMicrosecondType>(arr);
                        for row in 0..n_rows {
                            let value = PostgresDuration {
                                microseconds: arr.value(row),
                            };
                            write(&value)
                        }
                    }
                    time_unit => panic!("Durations in units of {time_unit:?} are not supported"),
                },
                DataType::Binary => {
                    let arr = arr.as_any().downcast_ref::<array::BinaryArray>().unwrap();
                    for v in arr.iter() {
                        write(&v)
                    }
                }
                DataType::FixedSizeBinary(_) => {
                    let arr = arr
                        .as_any()
                        .downcast_ref::<array::FixedSizeBinaryArray>()
                        .unwrap();
                    for v in arr.iter() {
                        write(&v)
                    }
                }
                DataType::LargeBinary => {
                    let arr = arr
                        .as_any()
                        .downcast_ref::<array::LargeBinaryArray>()
                        .unwrap();
                    for v in arr.iter() {
                        write(&v)
                    }
                }
                DataType::Utf8 => {
                    let arr = array::as_string_array(arr);
                    for v in arr.iter() {
                        write(&v)
                    }
                }
                DataType::LargeUtf8 => {
                    let arr = array::as_largestring_array(arr);
                    for v in arr.iter() {
                        write(&v)
                    }
                }
                _ => panic!("Unsupported data type"),
            }
        }
        // Iterate row-wise and accumulate the result
        buf.clear();
        buf.put_slice(MAGIC);
        buf.put_i32(0); // flags
        buf.put_i32(0); // header extension
        for row in 0..n_rows {
            buf.put_i16(n_cols as i16);
            for col in 0..n_cols {
                let idx = get_1d_index_from_row_col(row, col, n_rows);
                buf.extend_from_slice(&encoded[idx][..]);
            }
        }
        Ok(buf.split().freeze())
    }
    pub fn finish(&mut self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(-1);
        buf.split().freeze()
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use arrow::array;
    use arrow::array::{make_array, ArrayRef};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::RecordBatchReader;
    use rstest::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    fn run_snap_test(name: &str, batch: RecordBatch, schema: Schema) {
        let mut buff = BytesMut::new();
        let mut writer = ArrowToPostgresBinaryEncoder::new(schema);

        buff.put(writer.encode(batch).unwrap());
        buff.put(writer.finish());

        let snap_file =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("src/snapshots/{name}.bin"));
        if !snap_file.exists() {
            fs::write(snap_file.clone(), &buff[..]).unwrap();
            panic!("wrote new snap at {snap_file:?}")
        } else {
            let existing = fs::read(snap_file).unwrap();
            assert_eq!(existing, &buff[..])
        }
    }

    #[derive(Debug)]
    struct TestField {
        data_type: DataType,
        nullable: bool,
        data: ArrayRef,
    }

    impl TestField {
        fn new(data_type: DataType, nullable: bool, data: impl array::Array) -> Self {
            TestField {
                data_type,
                nullable,
                data: make_array(data.data().clone()),
            }
        }
    }

    fn create_batch_from_fields(fields: Vec<TestField>) -> (Schema, RecordBatch) {
        let schema = Schema::new(
            fields
                .iter()
                .enumerate()
                .map(|(i, f)| Field::new(format!("col_{i}"), f.data_type.clone(), f.nullable))
                .collect(),
        );
        let columns = fields.iter().map(|f| f.data.clone()).collect();
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns).unwrap();

        (schema, batch)
    }

    // macro_rules! type_tests {
    //     ($($name: ident: $value: expr,)*) => {
    //     $(
    //         #[test]
    //         fn $name() {
    //             let (schema, batch) = create_batch_from_fields($value);
    //             run_snap_test(stringify!($name), batch, schema);
    //         }
    //     )*
    //     }
    // }

    // type_tests! {
    //     // int8
    //     int8_non_nullable_no_options: vec![TestField::new(DataType::Int8, false, Int8Array::from(vec![-1, 0, 1]))],
    //     int8_non_nullable_with_options: vec![TestField::new(DataType::Int8, false, Int8Array::from(vec![Some(-1), Some(0), Some(1)]))],
    //     int8_nullable_with_no_options: vec![TestField::new(DataType::Int8, true, Int8Array::from(vec![-1, 0, 1]))],
    //     int8_nullable_with_nulls: vec![TestField::new(DataType::Int8, true, Int8Array::from(vec![Some(-1), Some(0), None]))],
    //     //
    //     timestamp_second_with_tz_non_nullable_no_options: vec![TestField::new(DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())), false, TimestampMillisecondArray::from(vec![
    //           1546214400000,
    //         1546214400000,
    //         -1546214400000,
    //     ]).with_timezone("America/New_York".to_string()))],
    //     timestamp_second_with_tz_non_nullable_with_options: vec![TestField::new(DataType::Int8, false, Int8Array::from(vec![Some(-1), Some(0), Some(1)]))],
    //     timestamp_second_with_tz_nullable_with_no_options: vec![TestField::new(DataType::Int8, true, Int8Array::from(vec![-1, 0, 1]))],
    //     timestamp_second_with_tz_nullable_with_nulls: vec![TestField::new(DataType::Int8, true, Int8Array::from(vec![Some(-1), Some(0), None]))],
    // }

    #[rstest]
    #[case::int8_non_nullable_no_options("int8_non_nullable_no_options", TestField::new(DataType::Int8, false, array::Int8Array::from(vec![-1, 0, 1])))]
    #[case::int64_non_nullable_no_options("int64_non_nullable_no_options", TestField::new(DataType::Int64, false, array::Int64Array::from(vec![-1, 0, 1])))]
    #[case::timestamp_second_with_tz_non_nullable_no_options("timestamp_second_with_tz_non_nullable_no_options", TestField::new(DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())), false, array::TimestampSecondArray::from(vec![
        0,
        1675210660,
        1675210661,
    ]).with_timezone("America/New_York".to_string())))]
    #[trace] //This attribute enable traceing
    fn test_field_types(#[case] name: &str, #[case] input: TestField) {
        let (schema, batch) = create_batch_from_fields(vec![input]);
        run_snap_test(name, batch, schema);
    }

    #[test]
    fn test_example_data() {
        let file = fs::File::open(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata/yellow_tripdata_2022-01.parquet")).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let mut reader = builder.build().unwrap();
        let record_batch = reader.next().unwrap().unwrap();
        run_snap_test("yellow_cab_tripdata", record_batch, Schema::new(reader.schema().fields().clone()))
    }
}
