// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

mod client;
mod model;

use pyo3::prelude::*;

#[pymodule]
fn ceresdb_client(_py: Python, m: &PyModule) -> PyResult<()> {
    unsafe {
        pyo3::ffi::PyEval_InitThreads();
    }

    client::register_py_module(m)?;
    model::register_py_module(m)?;

    Ok(())
}
