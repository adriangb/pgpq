# pgpq

![CI](https://github.com/adriangb/pgpq/actions/workflows/python.yaml/badge.svg)

Stream Apache Arrow RecordBatches into Postgres.

## Background

Postgres supports two bulk load formats: text (including CSV) and a custom binary format.
Loading data from CSVs is convenient but has a lot of problems:

- CSV has no standard for missing values. You have to configure the load with a specific string (or lack thereof) to interpret as null.
- CSV is untyped so you can end up loading a float as an int and such.
- There is no standard for quoting delimiters in text columns, leading to the need to escape characters or sanitize the data before loading it.
- CSV files don't natively support any sort of compression. This results in larger files in storage and more data transferred compared to compressible formats.

Other data systems, particularly data warehouses like BigQuery and Redshift, have robust support for exporting data to Parquet and CSV.
Exporting to CSV has the same pitfalls as loading from CSV, and sometimes even conflicting semantics for nulls, escaped delimiters, quotation and type conversions.

Since Postgres does not natively support loading from Parquet this library provides an io-free encoder that can convert from Parquet to Postgres' binary format on the fly.
It accepts Arrow data as an input which means great support for reading Parquet files from all sorts of sources (disk, HTTP, object stores, etc.) in an efficient and performant manner.

Benchmarks using the NYC Yellow Cab dataset show that it takes `pgpq` [less than 1 second to encode 1M rows](py/benches/encode.ipynb) and that the [cost of encoding + binary copy is lower than the cost of a native CSV copy](py/benches/copy.ipynb) (which ignores the cost of a CSV export if the data was a Parquet file in the first place).

## Python distribution

A Python wrapper that is published on PyPi.
It takes [pyarrow](https://arrow.apache.org/docs/python/index.html#) data as an input.

See [py](./py) for more info and a use example.

## Rust crate

The core is written in Rust and can be used in Rust-based projects.
It doesn't depend on any particular database driver and accepts [arrow-rs](https://github.com/apache/arrow-rs) objects as inputs.

See [core](./core).

## Data type support

We currently support nearly all scalar data types as well as List/Array data types.
There's no reason we can't support struct data types as well.

|   Arrow                   |   Postgres       |
|---------------------------|------------------|
|   Boolean                 |   BOOL           |
|   UInt8                   |   INT2           |
|   UInt16                  |   INT4           |
|   UInt32                  |   INT8           |
|   Int8                    |   CHAR,INT2      |
|   Int16                   |   INT2           |
|   Int32                   |   INT4           |
|   Int64                   |   INT8           |
|   Float16                 |   FLOAT4         |
|   Float32                 |   FLOAT4         |
|   Float64                 |   FLOAT8         |
|   Timestamp(Nanosecond)   |   Not supported  |
|   Timestamp(Microsecond)  |   TIMESTAMP      |
|   Timestamp(Millisecond)  |   TIMESTAMP      |
|   Timestamp(Second)       |   TIMESTAMP      |
|   Date32                  |   DATE           |
|   Date64                  |   DATE           |
|   Time32(Millisecond)     |   TIME           |
|   Time32(Second)          |   TIME           |
|   Time64(Nanosecond)      |   Not supported  |
|   Time64(Microsecond)     |   TIME           |
|   Duration(Nanosecond)    |   Not supported  |
|   Duration(Microsecond)   |   INTERVAL       |
|   Duration(Millisecond)   |   INTERVAL       |
|   Duration(Second)        |   INTERVAL       |
|   String                  |   TEXT,JSONB     |
|   Binary                  |   BYTEA          |
|   List\<T\>               |   Array\<T\>     |

### JSONB support

For more complex data types, like a struct with list fields, you might be better off dumping the data into a JSONB column. The [arrow-json rust crate](https://crates.io/crates/arrow-json) [arrow-json Python package](./json/README.md) provide support for converting arbitrary Arrow arrays into arrays of JSON strings, which can then be loaded into a JSONB column.
