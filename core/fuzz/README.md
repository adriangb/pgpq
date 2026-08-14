# pgpq fuzz targets

Structure-aware [`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz) targets for the `pgpq`
encoder. This crate is **not** part of the root workspace (it declares its own `[workspace]` and
the root `Cargo.toml` excludes it), so `cargo test` / `cargo clippy` on stable are unaffected.

## Why structure-aware?

`pgpq` does not parse untrusted bytes — its input is a typed Arrow `RecordBatch`. So the fuzzer's
byte stream is used as an entropy source (`arbitrary::Unstructured`) to *construct* a schema and
matching record batches, which are then driven through the real API. Coverage-guided mutation of
those bytes explores type combinations, null distributions, list/struct shapes and row counts that
a bounded property test cannot.

## Targets

### `encode_record_batch`

Builds an arbitrary `(Schema, Vec<RecordBatch>)` over the supported types (all ints/uints, floats,
`Decimal128`, timestamps, dates, times, durations, strings, binary, `List`/`LargeList` of scalars,
flat structs of scalars) and drives the full encode path:

```text
ArrowToPostgresBinaryEncoder::try_new -> schema().ddl() -> write_header -> write_batch* -> write_footer
```

The property is **"encoding valid Arrow data in the documented call order never panics"**. Encoder
`Err`s are values, not failures, and are ignored.

Two regions are deliberately excluded so the target explores new ground instead of rediscovering
known bugs on every run (both are documented with `KNOWN BUG` comments in
`core/tests/proptest_roundtrip.rs`):

* structs with a `List` field — `StructEncoderBuilder::try_new` panics because `PostgresType::List`
  has no OID;
* negative decimal scales — `Decimal*Encoder::byte_size_hint` underflows.

`write_header`/`write_batch`/`write_footer` `assert!` on out-of-order use. That is a deliberate
panic-on-misuse API, so the target always calls them in order; misuse is not the property under
test.

### No `json` crate target

The issue asked for a second target covering the `arrow-json` path in `json/src/lib.rs`. That crate
exposes **no callable Rust seam**: it is `crate-type = ["cdylib"]`, its single function
`array_to_utf8_json_array` is a `#[pyfunction]` that takes a `Python<'_>` token and a `pyarrow`
object, and there is no library target to depend on. Fuzzing it would mean either embedding a
Python interpreter or copying its body into the fuzz target — both worse than not having the
target. If the crate ever grows a plain Rust entry point (e.g. `pub fn array_to_json(array:
&dyn Array) -> Result<StringArray, ArrowError>` with the `#[pyfunction]` as a thin wrapper), a
target for it becomes a few lines.

## Running

`cargo-fuzz` needs a nightly toolchain (it relies on `-Z sanitizer`):

```bash
cargo install cargo-fuzz          # once
cargo +nightly fuzz build         # compile every target
cargo +nightly fuzz run encode_record_batch -- -max_total_time=60
```

Reproduce and minimise a crash:

```bash
cargo +nightly fuzz run encode_record_batch fuzz/artifacts/encode_record_batch/crash-<hash>
cargo +nightly fuzz tmin encode_record_batch fuzz/artifacts/encode_record_batch/crash-<hash>
```

A corpus is not committed; `cargo fuzz` will create `corpus/encode_record_batch/` on first run.
