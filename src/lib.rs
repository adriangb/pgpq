// use pyo3::create_exception;
// use pyo3::exceptions;
// use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::Python;

use arrow::datatypes::Schema;
use arrow::pyarrow::PyArrowConvert;
use arrow::record_batch::RecordBatch;

#[pyclass]
#[derive(Debug)]
struct ArrowToPostgresBinaryEncoder {
    encoder: pgpq::ArrowToPostgresBinaryEncoder,
}

#[pymethods]
impl ArrowToPostgresBinaryEncoder {
    #[new]
    fn new(pyschema: &PyAny) -> Self {
        ArrowToPostgresBinaryEncoder {
            encoder: pgpq::ArrowToPostgresBinaryEncoder::new(
                Schema::from_pyarrow(pyschema).unwrap(),
            ),
        }
    }
    fn encode(&mut self, batch: &PyAny) -> Py<PyAny> {
        let data = self
            .encoder
            .encode(RecordBatch::from_pyarrow(batch).unwrap())
            .unwrap();
        Python::with_gil(|py| PyBytes::new(py, &data[..]).into())
    }
    fn finish(&mut self, py: Python) -> Py<PyAny> {
        PyBytes::new(py, &self.encoder.finish()[..]).into()
    }
}

#[pymodule]
fn _pgpq(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<ArrowToPostgresBinaryEncoder>()?;
    Ok(())
}
