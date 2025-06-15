use arrow::array::Array;
use arrow::array::StringBuilder;
use pyo3::prelude::*;
use pyo3::{exceptions::PyValueError, Python};

use arrow::array::{make_array, ArrayData, LargeStringBuilder};
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use arrow_json::writer::Writer;
use serde_json::{to_string, Value};

#[pyfunction]
#[pyo3(signature = (array, large = true))]
fn array_to_utf8_json_array(
    py: Python,
    array: &Bound<'_, PyAny>,
    large: bool,
) -> PyResult<PyObject> {
    // This is super inefficient, leaving optimization as a TODO
    let array = make_array(ArrayData::from_pyarrow_bound(array)?);

    // Convert array to JSON values - we need to create a RecordBatch
    let schema = arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
        "json",
        array.data_type().clone(),
        true,
    )]);
    let batch = RecordBatch::try_new(std::sync::Arc::new(schema), vec![array.clone()])
        .map_err(|e| PyValueError::new_err(format!("Failed to create RecordBatch: {:?}", e)))?;

    let mut json_values = Vec::new();
    let mut json_writer = Writer::<_, arrow_json::writer::LineDelimited>::new(vec![]);
    json_writer
        .write(&batch)
        .map_err(|e| PyValueError::new_err(format!("Failed to convert array to JSON: {:?}", e)))?;
    json_writer
        .finish()
        .map_err(|e| PyValueError::new_err(format!("Failed to finish JSON writer: {:?}", e)))?;

    // Parse the JSON output to get individual values
    let json_bytes = json_writer.into_inner();
    let json_str = std::str::from_utf8(&json_bytes)
        .map_err(|e| PyValueError::new_err(format!("Invalid UTF-8 in JSON output: {:?}", e)))?;

    // Parse each line as a JSON value and extract the field
    for line in json_str.lines() {
        if !line.is_empty() {
            let obj: Value = serde_json::from_str(line).map_err(|e| {
                PyValueError::new_err(format!("Failed to parse JSON line: {:?}", e))
            })?;
            if let Some(value) = obj.get("json") {
                json_values.push(value.clone());
            }
        }
    }
    if large {
        let mut builder = LargeStringBuilder::new();
        for value in json_values.iter() {
            match value {
                Value::Null => builder.append_null(),
                value => builder.append_value(to_string(&value).map_err(|e| {
                    PyValueError::new_err(format!("Failed to serialize JSON value: {:?}", e))
                })?),
            }
        }
        let json_arr = builder.finish();
        json_arr.into_data().to_pyarrow(py)
    } else {
        let mut builder = StringBuilder::new();
        for value in json_values.iter() {
            match value {
                Value::Null => builder.append_null(),
                value => builder.append_value(to_string(&value).map_err(|e| {
                    PyValueError::new_err(format!("Failed to serialize JSON value: {:?}", e))
                })?),
            }
        }
        let json_arr = builder.finish();
        json_arr.into_data().to_pyarrow(py)
    }
}

#[pymodule]
fn _arrow_json(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(array_to_utf8_json_array, m)?)?;
    Ok(())
}
