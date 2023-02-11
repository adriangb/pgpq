#![allow(unused)]

use arrow::datatypes::{Schema, Int64Type};
use arrow_array::{RecordBatch, Array, PrimitiveArray};
use arrow_schema::{Field, DataType, TimeUnit};
use bytes::{BytesMut, BufMut};
use criterion::{criterion_group, criterion_main, Criterion};
use postgres_types::{ToSql, Type};
use std::sync::Arc;


pub fn benchmark_to_sql(c: &mut Criterion) {
    let mut group = c.benchmark_group("benchmark_to_sql");

    let n_rows = 10_000;

    let id_array = arrow_array::TimestampNanosecondArray::from((0..n_rows).collect::<Vec<i64>>());
    let batch = RecordBatch::try_new(Arc::new(Schema::new(
        vec![Field::new("c1", DataType::Timestamp(TimeUnit::Nanosecond, None), false)]
    )), vec![Arc::new(id_array)]).unwrap();

    group.bench_function("direct", |b| {
        b.iter(|| {
            let n_items = batch.num_columns() * batch.num_rows();
            let mut buff = BytesMut::zeroed(n_items * 8);
            for column in batch.columns() {
                match column.data_type() {
                    DataType::Timestamp(_,_) => {
                        let arr = column.as_any().downcast_ref::<arrow_array::TimestampNanosecondArray>().unwrap();
                        let arr: PrimitiveArray<Int64Type> = arr.unary(|v| v / 1_000);
                        for &v in arr.values() {
                            buff.put_i64(v);
                        }
                    }
                    _ => unreachable!()
                }
            }
        })
    });

    group.bench_function("direct-rows", |b| {
        b.iter(|| {
            let n_items = batch.num_columns() * batch.num_rows();
            let mut buff = BytesMut::zeroed(n_items * 8);
            for column in batch.columns() {
                match column.data_type() {
                    DataType::Timestamp(_,_) => {
                        let arr = column.as_any().downcast_ref::<arrow_array::TimestampNanosecondArray>().unwrap();
                        for &v in arr.values() {
                            buff.put_i64(v / 1000);
                        }
                    }
                    _ => unreachable!()
                }
            }
        })
    });

    group.bench_function("tosql", |b| {
        b.iter(|| {
            let n_items = batch.num_columns() * batch.num_rows();
            let mut buff = BytesMut::zeroed(n_items * 8);
            for column in batch.columns() {
                match column.data_type() {
                    DataType::Timestamp(_,_) => {
                        let arr = column.as_any().downcast_ref::<arrow_array::TimestampNanosecondArray>().unwrap();
                        for row in 0..arr.len() {
                            let val = arr.value_as_datetime(row).unwrap();
                            ToSql::to_sql(&val, &Type::TIMESTAMP, &mut buff).unwrap();
                        }
                    }
                    _ => unreachable!()
                }
            }
        })
    });
}

criterion_group!(benches, benchmark_to_sql);
criterion_main!(benches);
