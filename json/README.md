# arrow-json

Encode an arrow array into an array of json strings:

```python
import json
import pyarrow as pa
from arrow_json import array_to_utf8_json_array

data = [[{"f": [1, 2]}], [], None]
array = pa.array(data)
print(nested_list_struct_array.type)
# list<item: struct<f: list<item: int64>>>
json_array = array_to_utf8_json_array(array)
loaded = [json.loads(s) if s is not None else None for s in json_array.to_pylist()]
assert loaded == data
```
