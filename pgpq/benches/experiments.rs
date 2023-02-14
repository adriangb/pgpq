#![allow(unused)]

use arrow::array::ArrayIter;
use arrow::datatypes::{Schema, Int64Type};
use arrow::record_batch::RecordBatchReader;
use arrow_array::{Array, RecordBatch, PrimitiveArray};
use arrow_schema::{DataType, Field, TimeUnit};
use bytes::{BufMut, BytesMut};
use criterion::{criterion_group, criterion_main, Criterion, black_box};
use postgres_types::{Type, ToSql};
use std::fs;
use std::sync::Arc;

fn get_start_end(row: usize, col: usize, n_rows: usize, offsets: &[usize]) -> (usize, usize) {
    let idx = col * n_rows + col;
    (offsets[idx], offsets[idx + 1])
}

pub fn columnar_vs_row_wise(c: &mut Criterion) {
    let mut group = c.benchmark_group("benchmark_approaches");

    let n_rows = 10_000;

    let id_array = arrow_array::Int64Array::from((0..n_rows).collect::<Vec<i64>>());
    let string = "a".repeat(512);
    let name_array = arrow_array::StringArray::from(
        (0..n_rows).map(|_| string.clone()).collect::<Vec<String>>(),
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int64, false),
            Field::new("c2", DataType::Utf8, false),
        ])),
        vec![Arc::new(id_array), Arc::new(name_array)],
    )
    .unwrap();

    group.bench_function("columnar", |b| {
        b.iter(|| {
            let n_items = batch.num_columns() * batch.num_rows();
            let mut offsets: Vec<usize> = Vec::with_capacity(n_items);
            offsets.push(0);
            for column in batch.columns() {
                match column.data_type() {
                    DataType::Int64 => {
                        for row in 0..batch.num_rows() {
                            offsets.push(offsets.last().unwrap() + 8);
                        }
                    }
                    DataType::Utf8 => {
                        let arr = column
                            .as_any()
                            .downcast_ref::<arrow_array::StringArray>()
                            .unwrap();
                        for row in 0..arr.len() {
                            if !arr.is_null(row) {
                                let v = arr.value(row);
                                offsets.push(offsets.last().unwrap() + v.len());
                            } else {
                                panic!()
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            }
            let mut buff = BytesMut::zeroed(*offsets.last().unwrap());
            for (col, any_array) in batch.columns().iter().enumerate() {
                match any_array.data_type() {
                    DataType::Int64 => {
                        let arr = any_array
                            .as_any()
                            .downcast_ref::<arrow_array::Int64Array>()
                            .unwrap();
                        for row in 0..arr.len() {
                            if !arr.is_null(row) {
                                let v = arr.value(row);
                                let (start, end) =
                                    get_start_end(row, col, batch.num_rows(), &offsets);
                                buff[start..end].copy_from_slice(&v.to_be_bytes());
                            } else {
                                panic!()
                            };
                        }
                    }
                    DataType::Utf8 => {
                        let arr = any_array
                            .as_any()
                            .downcast_ref::<arrow_array::StringArray>()
                            .unwrap();
                        for row in 0..arr.len() {
                            if !arr.is_null(row) {
                                let v = arr.value(row).as_bytes();
                                let (start, end) =
                                    get_start_end(row, col, batch.num_rows(), &offsets);
                                buff[start..end].copy_from_slice(v);
                            } else {
                                panic!()
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            }
        })
    });

    group.bench_function("row-wise", |b| {
        b.iter(|| {
            let mut buffer = BytesMut::new();
            let cols = batch.columns();
            let additional = cols
                .iter()
                .map(|col| match col.data_type() {
                    DataType::Int64 => {
                        let arr = col
                            .as_any()
                            .downcast_ref::<arrow_array::Int64Array>()
                            .unwrap();
                        8 * arr.len()
                    }
                    DataType::Utf8 => {
                        let arr = col
                            .as_any()
                            .downcast_ref::<arrow_array::StringArray>()
                            .unwrap();
                        (arr.value_offsets().last().unwrap() - arr.value_offsets().first().unwrap())
                            as usize
                    }
                    _ => unreachable!(),
                })
                .sum();
            buffer.reserve(additional);
            for row in 0..batch.num_rows() {
                for col in cols {
                    match col.data_type() {
                        DataType::Int64 => {
                            let arr = col
                                .as_any()
                                .downcast_ref::<arrow_array::Int64Array>()
                                .unwrap();
                            if !arr.is_null(row) {
                                let v = arr.value(row);
                                buffer.put_i64(v);
                            } else {
                                panic!()
                            }
                        }
                        DataType::Utf8 => {
                            let arr = col
                                .as_any()
                                .downcast_ref::<arrow_array::StringArray>()
                                .unwrap();
                            if !arr.is_null(row) {
                                let v = arr.value(row).as_bytes();
                                buffer.put_slice(v);
                            } else {
                                panic!()
                            }
                        }
                        _ => unreachable!(),
                    }
                }
            }
        })
    });
}

pub fn benchmark_to_sql(c: &mut Criterion) {
    let mut group = c.benchmark_group("benchmark_to_sql");

    let n_rows = 10_000;

    let id_array = arrow_array::TimestampNanosecondArray::from((0..n_rows).collect::<Vec<i64>>());
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "c1",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )])),
        vec![Arc::new(id_array)],
    )
    .unwrap();

    group.bench_function("direct", |b| {
        b.iter(|| {
            let n_items = batch.num_columns() * batch.num_rows();
            let mut buff = BytesMut::zeroed(n_items * 8);
            for column in batch.columns() {
                match column.data_type() {
                    DataType::Timestamp(_, _) => {
                        let arr = column
                            .as_any()
                            .downcast_ref::<arrow_array::TimestampNanosecondArray>()
                            .unwrap();
                        let arr: PrimitiveArray<Int64Type> = arr.unary(|v| v / 1_000);
                        for &v in arr.values() {
                            buff.put_i64(v);
                        }
                    }
                    _ => unreachable!(),
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
                    DataType::Timestamp(_, _) => {
                        let arr = column
                            .as_any()
                            .downcast_ref::<arrow_array::TimestampNanosecondArray>()
                            .unwrap();
                        for &v in arr.values() {
                            buff.put_i64(v / 1000);
                        }
                    }
                    _ => unreachable!(),
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
                    DataType::Timestamp(_, _) => {
                        let arr = column
                            .as_any()
                            .downcast_ref::<arrow_array::TimestampNanosecondArray>()
                            .unwrap();
                        for row in 0..arr.len() {
                            let val = arr.value_as_datetime(row).unwrap();
                            ToSql::to_sql(&val, &Type::TIMESTAMP, &mut buff).unwrap();
                        }
                    }
                    _ => unreachable!(),
                }
            }
        })
    });
}


pub fn direct_byte_access_vs_string_wrapped(c: &mut Criterion) {
    let mut group = c.benchmark_group("direct_byte_access_vs_string_wrapped");

    let data = vec![b"foo!".repeat(1_000); 1_000];

    group.bench_function("direct", |b| {
        b.iter(|| {
            for bytes in &data {
                let v = &bytes[..];
                black_box(v);
            }
        })
    });

    group.bench_function("to_string", |b| {
        b.iter(|| {
            for bytes in &data {
                let v = (unsafe { std::str::from_utf8_unchecked(&bytes[..]) }).as_bytes();
                black_box(v);
            }

        })
    });
}

criterion_group!(benches, columnar_vs_row_wise, benchmark_to_sql, direct_byte_access_vs_string_wrapped);
criterion_main!(benches);
