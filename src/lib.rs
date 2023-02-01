use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::Python;

use arrow::datatypes::Schema;
use arrow::pyarrow::PyArrowConvert;
use arrow::record_batch::RecordBatch;
use bytes::BytesMut;

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
        let mut buff = BytesMut::new();
        self.encoder
            .encode(RecordBatch::from_pyarrow(batch).unwrap(), &mut buff)
            .unwrap();
        Python::with_gil(|py| PyBytes::new(py, &buff[..]).into())
    }
    fn finish(&mut self, py: Python) -> Py<PyAny> {
        let mut buff = BytesMut::new();
        self.encoder.finish(&mut buff).unwrap();
        PyBytes::new(py, &buff[..]).into()
    }
}

#[pymodule]
fn _pgpq(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<ArrowToPostgresBinaryEncoder>()?;
    Ok(())
}
