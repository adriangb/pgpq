#![allow(unused)]

use arrow::array::ArrayIter;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatchReader;
use arrow_array::{RecordBatch, Array};
use arrow_schema::{Field, DataType};
use bytes::{BytesMut, BufMut};
use criterion::{criterion_group, criterion_main, Criterion};
use std::fs;
use std::sync::Arc;


pub fn benchmark_approaches(c: &mut Criterion) {
    let mut group = c.benchmark_group("benchmark_approaches");

    let n_rows = 10_000;

    let id_array = arrow_array::Int64Array::from((0..n_rows).collect::<Vec<i64>>());
    let string = "a".repeat(512);
    let name_array = arrow_array::StringArray::from((0..n_rows).map(|_| string.clone()).collect::<Vec<String>>());
    let batch = RecordBatch::try_new(Arc::new(Schema::new(
        vec![Field::new("c1", DataType::Int64, false), Field::new("c2", DataType::Utf8, false)]
    )), vec![Arc::new(id_array), Arc::new(name_array)]).unwrap();

    group.bench_function("columnar", |b| {
        b.iter(|| {
            let mut buffer = BytesMut::new();
            let n_items = batch.num_columns() * batch.num_rows();
            let mut offsets: Vec<usize> = Vec::with_capacity(n_items);
            offsets.push(0);
            for column in batch.columns() {
                match column.data_type() {
                    DataType::Int64 => {
                        let arr = column.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
                        for row in 0..arr.len() {
                            if !arr.is_null(row) {
                                let v = arr.value(row);
                                buffer.put_i64(v);
                                offsets.push(buffer.len());
                            } else { panic!()};
                        }
                    }
                    DataType::Utf8 => {
                        let arr = column.as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
                        for row in 0..arr.len() {
                            if !arr.is_null(row) {
                                let v = arr.value(row).as_bytes();
                                buffer.put_slice(v);
                                offsets.push(buffer.len());
                            } else { panic!()}
                        }
                    }
                    _ => unreachable!()
                }
            }
            let mut output = BytesMut::with_capacity(buffer.len());
            for row in 0..batch.num_rows() {
                for col in 0..batch.num_columns() {
                    let idx = col * batch.num_rows() + col;
                    let start = offsets[idx];
                    let end = offsets[idx+1];
                    output.put_slice(&buffer[start..end])
                }
            }
        })
    });

    group.bench_function("row-wise", |b| {
        b.iter(|| {
            let mut buffer = BytesMut::new();
            let cols = batch.columns();
            for row in 0..batch.num_rows() {
                for col in cols {
                    match col.data_type() {
                        DataType::Int64 => {
                            let arr = col.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
                            if !arr.is_null(row) {
                                let v = arr.value(row);
                                buffer.put_i64(v);
                            } else { panic!()}
                        }
                        DataType::Utf8 => {
                            let arr = col.as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
                            if !arr.is_null(row) {
                                let v = arr.value(row).as_bytes();
                                buffer.put_slice(v);
                            } else { panic!()}
                        }
                        _ => unreachable!()
                    }
                }
            }
        })
    });

}

criterion_group!(benches, benchmark_approaches);
criterion_main!(benches);
