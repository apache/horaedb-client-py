// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use ceresdb_client_rs::{model as rust_model, model::QueriedRows};
use common_types::datum::Datum;
use pyo3::{exceptions::PyTypeError, prelude::*};

pub fn register_py_module(m: &PyModule) -> PyResult<()> {
    m.add_class::<QueryRequest>()?;
    m.add_class::<QueryResponse>()?;
    m.add_class::<ColumnSchema>()?;
    m.add_class::<ColumnDataType>()?;
    m.add_class::<Schema>()?;
    m.add_class::<Row>()?;

    Ok(())
}

#[pyclass]
#[derive(Debug)]
pub struct QueryRequest {
    pub metrics: Vec<String>,
    pub ql: String,
}

#[pymethods]
impl QueryRequest {
    #[new]
    pub fn new(metrics: Vec<String>, ql: String) -> Self {
        Self { metrics, ql }
    }

    pub fn __str__(&self) -> String {
        format!("{:?}", self)
    }
}

#[pyclass]
#[derive(Clone, Copy, Debug)]
pub enum ColumnDataType {
    Null = 0,
    TimestampMillis,
    Double,
    Float,
    Bytes,
    String,
    Int64,
    Int32,
    Boolean,
}

impl From<rust_model::ColumnDataType> for ColumnDataType {
    fn from(typ: rust_model::ColumnDataType) -> Self {
        match typ {
            rust_model::ColumnDataType::Null => ColumnDataType::Null,
            rust_model::ColumnDataType::TimestampMillis => ColumnDataType::TimestampMillis,
            rust_model::ColumnDataType::Double => ColumnDataType::Double,
            rust_model::ColumnDataType::Float => ColumnDataType::Float,
            rust_model::ColumnDataType::Bytes => ColumnDataType::Bytes,
            rust_model::ColumnDataType::String => ColumnDataType::String,
            rust_model::ColumnDataType::Int64 => ColumnDataType::Int64,
            rust_model::ColumnDataType::Int32 => ColumnDataType::Int32,
            rust_model::ColumnDataType::Boolean => ColumnDataType::Boolean,
        }
    }
}

#[pyclass]
#[derive(Debug)]
pub struct Row {
    idx: usize,
    raw_rows: Arc<Vec<rust_model::Row>>,
}

#[pymethods]
impl Row {
    pub fn get_column_value(&self, py: Python<'_>, idx: usize) -> PyResult<PyObject> {
        let raw_row = &self.raw_rows[self.idx];
        let datum = match raw_row.datums.get(idx) {
            Some(v) => v,
            None => {
                return Err(PyTypeError::new_err(format!(
                    "invalid column idx:{}, total columns:{}",
                    idx,
                    raw_row.datums.len()
                )))
            }
        };
        let col_val = match datum {
            Datum::Null => py.None(),
            Datum::Timestamp(v) => v.as_i64().to_object(py),
            Datum::Double(v) => v.to_object(py),
            Datum::Float(v) => v.to_object(py),
            Datum::Varbinary(v) => v.as_ref().to_object(py),
            Datum::String(v) => v.as_str().to_object(py),
            Datum::Int64(v) => v.to_object(py),
            Datum::Int32(v) => v.to_object(py),
            Datum::Boolean(v) => v.to_object(py),
            Datum::UInt64(_)
            | Datum::UInt32(_)
            | Datum::UInt16(_)
            | Datum::UInt8(_)
            | Datum::Int16(_)
            | Datum::Int8(_) => {
                return Err(PyTypeError::new_err(format!(
                    "Unsupported datum type:{:?}, idx:{}",
                    datum, idx
                )))
            }
        };

        Ok(col_val)
    }

    pub fn __str__(&self) -> String {
        format!("{:?}", self)
    }
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct ColumnSchema {
    raw_schema: Arc<rust_model::ColumnSchema>,
}

#[pymethods]
impl ColumnSchema {
    pub fn name(&self) -> &str {
        &self.raw_schema.name
    }

    pub fn data_type(&self) -> ColumnDataType {
        ColumnDataType::from(self.raw_schema.data_type)
    }

    pub fn __str__(&self) -> String {
        format!("{:?}", self)
    }
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct Schema {
    raw_schema: Arc<rust_model::Schema>,
}

#[pymethods]
impl Schema {
    pub fn num_cols(&self) -> usize {
        self.raw_schema.num_cols()
    }

    pub fn col_idx(&self, name: &str) -> Option<usize> {
        self.raw_schema.col_idx(name)
    }

    pub fn get_column_schema(&self, idx: usize) -> Option<ColumnSchema> {
        self.raw_schema
            .column_schemas
            .get(idx)
            .map(|v| ColumnSchema {
                raw_schema: Arc::new(v.clone()),
            })
    }

    pub fn __str__(&self) -> String {
        format!("{:?}", self)
    }
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct QueryResponse {
    schema: Schema,
    raw_rows: Arc<Vec<rust_model::Row>>,
}

#[pymethods]
impl QueryResponse {
    #[new]
    pub fn new(schema: Schema, rows: Vec<PyRef<Row>>) -> Self {
        let mut raw_rows = Vec::with_capacity(rows.len());
        for row in rows {
            raw_rows.push(row.raw_rows[row.idx].clone());
        }
        Self {
            schema,
            raw_rows: Arc::new(raw_rows),
        }
    }

    pub fn schema(&self) -> Schema {
        self.schema.clone()
    }

    pub fn row_num(&self) -> usize {
        self.raw_rows.len()
    }

    pub fn get_row(&self, idx: usize) -> Option<Row> {
        if self.raw_rows.len() > idx {
            Some(Row {
                raw_rows: self.raw_rows.clone(),
                idx,
            })
        } else {
            None
        }
    }

    pub fn __str__(&self) -> String {
        format!("{:?}", self)
    }
}

pub fn convert_queried_rows(queried_rows: QueriedRows) -> PyResult<QueryResponse> {
    Ok(QueryResponse {
        schema: Schema {
            raw_schema: Arc::new(queried_rows.schema),
        },
        raw_rows: Arc::new(queried_rows.rows),
    })
}
