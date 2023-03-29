use pyo3::Python;

pub(crate) trait PythonRepr {
    fn py_repr(&self, py: Python) -> String;
}
