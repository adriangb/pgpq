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
    empty: Py<PyAny>,
}

const BUFF_SIZE: usize = 1024 * 1024;

#[pymethods]
impl ArrowToPostgresBinaryEncoder {
    #[new]
    fn new(pyschema: &PyAny, py: Python) -> Self {
        let encoder =
            pgpq::ArrowToPostgresBinaryEncoder::try_new(&Schema::from_pyarrow(pyschema).unwrap())
                .unwrap();
        ArrowToPostgresBinaryEncoder {
            encoder,
            buf: BytesMut::with_capacity(BUFF_SIZE),
            empty: PyBytes::new(py, &vec![][..]).into(),
        }
    }
    fn write_header(&mut self, py: Python) -> Py<PyAny> {
        self.encoder.write_header(&mut self.buf);
        PyBytes::new(py, &self.buf.split()[..]).into()
    }
    fn write_batch(&mut self, batch: &PyAny) -> Py<PyAny> {
        self.encoder
            .write_batch(&RecordBatch::from_pyarrow(batch).unwrap(), &mut self.buf)
            .unwrap();
        if self.buf.len() > BUFF_SIZE {
            Python::with_gil(|py| PyBytes::new(py, &self.buf.split()[..]).into())
        } else {
            self.empty.clone()
        }
    }
    fn finish(&mut self) -> &[u8] {
        self.encoder.write_footer(&mut self.buf).unwrap();
        &self.buf[..]
    }
}

#[pymodule]
fn _pgpq(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<ArrowToPostgresBinaryEncoder>()?;
    Ok(())
}
