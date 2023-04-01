"""Example for README.md"""
from shutil import rmtree
from tempfile import mkdtemp
import psycopg
import pyarrow.dataset as ds
import requests
from pgpq import ArrowToPostgresBinaryEncoder


# let's get some example data
tmpdir = mkdtemp()
with open(f"{tmpdir}/yellow_tripdata_2023-01.parquet", mode="wb") as f:
    resp = requests.get(
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    )
    resp.raise_for_status()
    f.write(resp.content)

# load an arrow dataset
# arrow can load datasets from partitioned parquet files locally or in S3/GCS
# it handles buffering, matching globs, etc.
dataset = ds.dataset(tmpdir)

# create an encoder object which will do the encoding
# and give us the expected Postgres table schema
encoder = ArrowToPostgresBinaryEncoder(dataset.schema)
# get the expected Postgres destination schema
# note that this is _not_ the same as the incoming arrow schema
# and not necessarily the schema of your permanent table
# instead it's the schema of the data that will be sent over the wire
# which for example does not have timezones on any timestamps
pg_schema = encoder.schema()
# assemble ddl for a temporary table
# it's often a good idea to bulk load into a temp table to:
# (1) Avoid indexes
# (2) Stay in-memory as long as possible
# (3) Be more flexible with types
#     (you can't load a SMALLINT into a BIGINT column without casting)
cols = [f'"{col_name}" {col.data_type.ddl()}' for col_name, col in pg_schema.columns]
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

rmtree(tmpdir)
