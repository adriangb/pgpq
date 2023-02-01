from __future__ import annotations

from pyarrow import Schema, RecordBatch  # type: ignore


class ArrowToPostgresBinaryEncoder:
    def __init__(self, schema: Schema) -> None:  # type: ignore
        ...
    
    def encode(self, __batch: RecordBatch) -> bytes:  # type: ignore
        ...

    def finish(self) -> bytes:
        ...
