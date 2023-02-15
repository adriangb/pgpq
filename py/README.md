# pgpq

Convert PyArrow RecordBatches to Postgres' native binary format.

## Usage

```python
import psycopg
import pyarrow.dataset as ds
from pgpq import ArrowToPostgresBinaryEncoder


dataset = ds.dataset("tests/testdata/")

encoder = ArrowToPostgresBinaryEncoder(dataset.schema)
# get the expected postgres destination schema
# note that this is _not_ the same as the incoming arrow schema
# and not necessarily the schema of your permanent table
# instead it's the schema of the data that will be sent over the wire
# which for example does not have timezones on any timestamps
pg_schema = encoder.schema()
# assemble ddl for a temporary table
# it's often a good idea to bulk load into a temp table to:
# (1) Avoid indexes
# (2) Stay in-memory as long as possible
# (3) Be more flexible with types (you can't load a SMALLINT into a BIGINT column without casting)
cols = [f"\"{col['name']}\" {col['data_type']['ddl']}" for col in pg_schema["columns"]]
ddl = f"CREATE TEMP TABLE data ({','.join(cols)})"

with psycopg.connect("postgres://postgres:postgres@localhost:5432/postgres") as conn:
    with conn.cursor() as cursor:
        cursor.execute(ddl)  # type: ignore
        with cursor.copy("COPY data FROM STDIN WITH (FORMAT BINARY)") as copy:
            copy.write(encoder.write_header())
            for batch in dataset.to_batches():
                copy.write(encoder.write_batch(batch))
            copy.write(encoder.finish())
        # load into your actual table, possibly doing type casts
        # cursor.execute("INSERT INTO \"table\" SELECT * FROM data")
```
