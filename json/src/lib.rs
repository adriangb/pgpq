use arrow::array::StringBuilder;
use pyo3::prelude::*;
use pyo3::Python;

use arrow::array::{make_array, Array, ArrayData, LargeStringBuilder};
use arrow::json::writer::array_to_json_array;
use arrow::pyarrow::PyArrowConvert;
use serde_json::{to_string, Value};

#[pyfunction]
#[pyo3(signature = (array, large))]
fn array_to_utf8_json_array(py: Python, array: &PyAny, large: bool) -> PyResult<PyObject> {
    // This is super inefficient, leaving optimization as a TODO
    let array = make_array(ArrayData::from_pyarrow(array)?);
    let json = array_to_json_array(&array).unwrap();
    if large {
        let mut builder = LargeStringBuilder::new();
        for value in json.into_iter() {
            match value {
                Value::Null => builder.append_null(),
                value => builder.append_value(to_string(&value).unwrap()),
            }
        }
        let json_arr = builder.finish();
        json_arr.data().to_pyarrow(py)
    } else {
        let mut builder = StringBuilder::new();
        for value in json.into_iter() {
            match value {
                Value::Null => builder.append_null(),
                value => builder.append_value(to_string(&value).unwrap()),
            }
        }
        let json_arr = builder.finish();
        json_arr.data().to_pyarrow(py)
    }
}

#[pymodule]
fn _arrow_json(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(array_to_utf8_json_array, m)?)?;
    Ok(())
}
