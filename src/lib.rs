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
    buf: BytesMut,
}

const BUFF_SIZE: usize = 1024 * 1024;
const EMPTY: Vec<u8> = vec![];

#[pymethods]
impl ArrowToPostgresBinaryEncoder {
    #[new]
    fn new(pyschema: &PyAny) -> Self {
        let encoder =
            pgpq::ArrowToPostgresBinaryEncoder::new(Schema::from_pyarrow(pyschema).unwrap());
        ArrowToPostgresBinaryEncoder {
            encoder,
            buf: BytesMut::with_capacity(BUFF_SIZE),
        }
    }
    fn write_header(&mut self, py: Python) -> Py<PyAny> {
        self.encoder.write_header(&mut self.buf);
        PyBytes::new(py, &self.buf.split()[..]).into()
    }
    fn write_batch(&mut self, batch: &PyAny) -> Py<PyAny> {
        self.encoder
            .write_batch(RecordBatch::from_pyarrow(batch).unwrap(), &mut self.buf)
            .unwrap();
        Python::with_gil(|py| {
            if self.buf.len() > BUFF_SIZE {
                PyBytes::new(py, &self.buf.split()[..]).into()
            } else {
                PyBytes::new(py, &EMPTY[..]).into()
            }
        })
    }
    fn finish(&mut self, py: Python) -> Py<PyAny> {
        self.encoder.write_footer(&mut self.buf).unwrap();
        PyBytes::new(py, &self.buf[..]).into()
    }
}

#[pymodule]
fn _pgpq(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<ArrowToPostgresBinaryEncoder>()?;
    Ok(())
}
