# pgpq

![CI](https://github.com/adriangb/pgpq/actions/workflows/python.yaml/badge.svg)

Stream Apache Arrow RecordBatches into Postgres.

## Background

Postgres supports two bulk load formats: text (including CSV) and a custom binary format.
Loading data from CSVs is convenient but has a lot of problems:

- CSV has no standard for missing values. You have to configure the load with a specific string (or lack thereof) to interpret as null.
- CSV is untyped so you can end up loading a float as an int and such.
- There is no standard for quoting delimiters in text columns, leading to the need to escape characters or sanitize the data before loading it.
- CSV files don't natively support any sort of compression. This results in larger files in storage and more data transfered compared to compressible formats.

Other data systems, particularly data warehouses like BigQuery and Redshift, have robust support for exporting data to Parquet and CSV.
Exporting to CSV has the same pitfalls as loading from CSV, and sometimes even conflicting semantics for nulls, escaped delimiters, quotation and type conversions.

Since Postgres does not natively support loading from Parquet this library provides an io-free encoder that can convert from Parquet to Postgres' binary format on the fly.
It accepts Arrow data as an input which means great support for reading Parquet files from all sorts of sources (disk, HTTP, object stores, etc.) in an efficient and performance manner.

## Python distribution

A Python wrapper that is published on PyPi.
It takes [pyarrow](https://arrow.apache.org/docs/python/index.html#) data as an input.

See [py](./py) for more info and a use example.

## Rust crate

The core is written in Rust and can be used in Rust-based projects.
It doesn't depend on any particular database driver and accepts [arrow-rs](https://github.com/apache/arrow-rs) objects as inputs.

See [core](./core).
