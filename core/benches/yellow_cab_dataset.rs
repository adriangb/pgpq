#![allow(unused)]

use arrow::array::ArrayIter;
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::record_batch::RecordBatchReader;
use arrow_array::RecordBatch;
use bytes::BytesMut;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use pgpq::ArrowToPostgresBinaryEncoder;
use std::fs;
use std::fs::File;
use std::io;
use std::path::PathBuf;

fn download_dataset() -> File {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("yellow_tripdata_2022-01.parquet");
    if !path.exists() {
        let mut file = File::create(path.clone()).expect("failed to create file");
        let mut resp = ureq::get(
            "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet",
        )
        .call()
        .expect("request failed");
        io::copy(&mut resp.into_reader(), &mut file).expect("failed to copy content");
    }

    File::open(path).expect("failed to create file")
}

fn setup(row_limit: Option<usize>) -> (Vec<RecordBatch>, Schema) {
    let mut file = download_dataset();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let mut reader = builder.build().unwrap();
    let schema = Schema::new(reader.schema().fields().clone());
    let data: Vec<RecordBatch> = match row_limit {
        Some(n) => reader.take(n).map(|v| v.unwrap()).collect(),
        None => reader.map(|v| v.unwrap()).collect(),
    };
    (data, schema)
}

fn bench(batches: &Vec<RecordBatch>, schema: &Schema) {
    let mut encoder = ArrowToPostgresBinaryEncoder::try_new(schema).unwrap();
    let mut buff = BytesMut::new();
    encoder.write_header(&mut buff);
    for batch in batches {
        encoder.write_batch(batch, &mut buff);
    }
    encoder.write_footer(&mut buff);
}

pub fn benchmark_nyc_taxi_small(c: &mut Criterion) {
    let mut group = c.benchmark_group("benchmark_nyc_taxi_small");
    let (batches, schema) = setup(Some(100));
    group.bench_function("NYC Yello Taxi 100 rows", |b| {
        b.iter(|| bench(&batches, &schema))
    });
}

pub fn benchmark_nyc_taxi_full(c: &mut Criterion) {
    let mut group = c.benchmark_group("benchmark_nyc_taxi_full");
    group.sampling_mode(criterion::SamplingMode::Flat);
    group.sample_size(10); // the minimum

    let (batches, schema) = setup(Some(100));
    group.bench_function("NYC Yello Taxi full", |b| {
        b.iter(|| bench(&batches, &schema))
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = benchmark_nyc_taxi_small, benchmark_nyc_taxi_full
}
criterion_main!(benches);
