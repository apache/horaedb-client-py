// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Read/Write request and response, and useful tools for them.

use std::sync::Arc;

use ceresdb_client_rs::{
    model as rust_model,
    model::{
        value::{TimestampMs, Value as RustValue},
        write::{is_reserved_column_name, WriteRequestBuilder, WriteResponse as RustWriteResponse},
        Datum, QueryResponse as RustQueryResponse,
    },
};
use pyo3::{class::basic::CompareOp, exceptions::PyTypeError, prelude::*};

pub fn register_py_module(m: &PyModule) -> PyResult<()> {
    m.add_class::<QueryRequest>()?;
    m.add_class::<QueryResponse>()?;
    m.add_class::<ColumnSchema>()?;
    m.add_class::<ColumnDataType>()?;
    m.add_class::<Schema>()?;
    m.add_class::<Row>()?;
    m.add_class::<Value>()?;
    m.add_class::<ValueBuilder>()?;
    m.add_class::<PointBuilder>()?;
    m.add_class::<Point>()?;
    m.add_class::<WriteRequest>()?;
    m.add_class::<WriteResponse>()?;

    m.add("ColumnDataTypeNull", ColumnDataType(COLUMN_DATA_TYPE_NULL))?;
    m.add(
        "ColumnDataTypeTimestampMillis",
        ColumnDataType(COLUMN_DATA_TYPE_TIMESTAMP_MILLIS),
    )?;
    m.add(
        "ColumnDataTypeDouble",
        ColumnDataType(COLUMN_DATA_TYPE_DOUBLE),
    )?;
    m.add(
        "ColumnDataTypeFloat",
        ColumnDataType(COLUMN_DATA_TYPE_FLOAT),
    )?;
    m.add(
        "ColumnDataTypeBytes",
        ColumnDataType(COLUMN_DATA_TYPE_BYTES),
    )?;
    m.add(
        "ColumnDataTypeString",
        ColumnDataType(COLUMN_DATA_TYPE_STRING),
    )?;
    m.add(
        "ColumnDataTypeInt64",
        ColumnDataType(COLUMN_DATA_TYPE_INT64),
    )?;
    m.add(
        "ColumnDataTypeInt32",
        ColumnDataType(COLUMN_DATA_TYPE_INT32),
    )?;
    m.add(
        "ColumnDataTypeBoolean",
        ColumnDataType(COLUMN_DATA_TYPE_BOOLEAN),
    )?;

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
pub struct ColumnDataType(u8);

pub const COLUMN_DATA_TYPE_NULL: u8 = 0;
pub const COLUMN_DATA_TYPE_TIMESTAMP_MILLIS: u8 = 1;
pub const COLUMN_DATA_TYPE_DOUBLE: u8 = 2;
pub const COLUMN_DATA_TYPE_FLOAT: u8 = 3;
pub const COLUMN_DATA_TYPE_BYTES: u8 = 4;
pub const COLUMN_DATA_TYPE_STRING: u8 = 5;
pub const COLUMN_DATA_TYPE_INT64: u8 = 6;
pub const COLUMN_DATA_TYPE_INT32: u8 = 7;
pub const COLUMN_DATA_TYPE_BOOLEAN: u8 = 8;

impl ToString for ColumnDataType {
    fn to_string(&self) -> String {
        let typ_desc = match self.0 {
            COLUMN_DATA_TYPE_NULL => "Null",
            COLUMN_DATA_TYPE_TIMESTAMP_MILLIS => "TimestampMills",
            COLUMN_DATA_TYPE_DOUBLE => "Double",
            COLUMN_DATA_TYPE_FLOAT => "Float",
            COLUMN_DATA_TYPE_BYTES => "Bytes",
            COLUMN_DATA_TYPE_STRING => "String",
            COLUMN_DATA_TYPE_INT64 => "Int64",
            COLUMN_DATA_TYPE_INT32 => "Int32",
            COLUMN_DATA_TYPE_BOOLEAN => "Boolean",
            _ => return format!("Unknown data type:{}", self.0),
        };

        typ_desc.to_string()
    }
}

#[pymethods]
impl ColumnDataType {
    pub fn __str__(&self) -> String {
        self.to_string()
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Lt => Ok(self.0 < other.0),
            CompareOp::Le => Ok(self.0 <= other.0),
            CompareOp::Eq => Ok(self.0 == other.0),
            CompareOp::Ne => Ok(self.0 != other.0),
            CompareOp::Gt => Ok(self.0 > other.0),
            CompareOp::Ge => Ok(self.0 >= other.0),
        }
    }
}

impl From<rust_model::ColumnDataType> for ColumnDataType {
    fn from(typ: rust_model::ColumnDataType) -> Self {
        let typ_num = typ as u8;
        Self(typ_num)
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
    #[pyo3(get)]
    affected_rows: u32,
}

#[pymethods]
impl QueryResponse {
    #[new]
    pub fn new(schema: Schema, rows: Vec<PyRef<Row>>, affected_rows: u32) -> Self {
        let mut raw_rows = Vec::with_capacity(rows.len());
        for row in rows {
            raw_rows.push(row.raw_rows[row.idx].clone());
        }
        Self {
            schema,
            raw_rows: Arc::new(raw_rows),
            affected_rows,
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

pub fn convert_query_response(query_resp: RustQueryResponse) -> PyResult<QueryResponse> {
    Ok(QueryResponse {
        schema: Schema {
            raw_schema: Arc::new(query_resp.schema),
        },
        raw_rows: Arc::new(query_resp.rows),
        affected_rows: query_resp.affected_rows,
    })
}

/// Value in local, define to avoid using the one in ceresdb.
#[pyclass]
#[derive(Clone, Debug)]
pub struct Value {
    raw_val: rust_model::value::Value,
}

#[pyclass]
#[derive(Clone, Debug, Default)]
pub struct ValueBuilder;

#[pymethods]
impl ValueBuilder {
    #[new]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_i8(&self, val: i8) -> Value {
        Value {
            raw_val: RustValue::Int8(val),
        }
    }

    pub fn with_u8(&self, val: u8) -> Value {
        Value {
            raw_val: RustValue::UInt8(val),
        }
    }

    pub fn with_i16(&self, val: i16) -> Value {
        Value {
            raw_val: RustValue::Int16(val),
        }
    }

    pub fn with_u16(&self, val: u16) -> Value {
        Value {
            raw_val: RustValue::UInt16(val),
        }
    }

    pub fn with_i32(&self, val: i32) -> Value {
        Value {
            raw_val: RustValue::Int32(val),
        }
    }

    pub fn with_u32(&self, val: u32) -> Value {
        Value {
            raw_val: RustValue::UInt32(val),
        }
    }

    pub fn with_i64(&self, val: i64) -> Value {
        Value {
            raw_val: RustValue::Int64(val),
        }
    }

    pub fn with_u64(&self, val: u64) -> Value {
        Value {
            raw_val: RustValue::UInt64(val),
        }
    }

    pub fn with_float(&self, val: f32) -> Value {
        Value {
            raw_val: RustValue::Float(val),
        }
    }

    pub fn with_double(&self, val: f64) -> Value {
        Value {
            raw_val: RustValue::Double(val),
        }
    }

    pub fn with_bool(&self, val: bool) -> Value {
        Value {
            raw_val: RustValue::Boolean(val),
        }
    }

    pub fn with_str(&self, val: String) -> Value {
        Value {
            raw_val: RustValue::String(val),
        }
    }

    pub fn with_varbinary(&self, val: Vec<u8>) -> Value {
        Value {
            raw_val: RustValue::Varbinary(val),
        }
    }
}

/// Point represents one data row needed to write.
#[pyclass]
#[derive(Clone, Debug)]
pub struct Point {
    metric: String,
    timestamp: TimestampMs,
    tags: Vec<(String, Value)>,
    fields: Vec<(String, Value)>,
}

#[pyclass]
#[derive(Clone, Default)]
pub struct PointBuilder {
    metric: Option<String>,
    timestamp: Option<TimestampMs>,
    tags: Vec<(String, Value)>,
    fields: Vec<(String, Value)>,
    contains_reserved_column_name: bool,
}

#[pymethods]
impl PointBuilder {
    #[new]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn metric(&mut self, metric: String) {
        self.metric = Some(metric);
    }

    pub fn timestamp(&mut self, timestamp: TimestampMs) {
        self.timestamp = Some(timestamp);
    }

    pub fn tag(&mut self, name: String, val: Value) {
        if is_reserved_column_name(&name) {
            self.contains_reserved_column_name = true;
        }
        self.tags.push((name, val));
    }

    pub fn field(&mut self, name: String, val: Value) {
        if is_reserved_column_name(&name) {
            self.contains_reserved_column_name = true;
        }
        self.fields.push((name, val));
    }

    pub fn build(&mut self) -> PyResult<Point> {
        if self.contains_reserved_column_name {
            return Err(PyTypeError::new_err(
                "Tag or field name contains reserved column name in ceresdb".to_string(),
            ));
        }

        if self.fields.is_empty() {
            return Err(PyTypeError::new_err(
                "Fields should not be empty".to_string(),
            ));
        }

        Ok(Point {
            metric: std::mem::take(&mut self.metric)
                .ok_or_else(|| PyTypeError::new_err("Metric must be set".to_string()))?,
            timestamp: self
                .timestamp
                .ok_or_else(|| PyTypeError::new_err("Timestamp must be set".to_string()))?,
            tags: std::mem::take(&mut self.tags),
            fields: std::mem::take(&mut self.fields),
        })
    }
}

/// A wrapper for `WriteRequestBuilder`.
#[pyclass]
#[derive(Clone, Default)]
pub struct WriteRequest {
    builder: WriteRequestBuilder,
}

#[pymethods]
impl WriteRequest {
    #[new]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_point(&mut self, point: Point) -> PyResult<()> {
        let mut row_builder = self.builder.row_builder();

        row_builder = row_builder.metric(point.metric).timestamp(point.timestamp);
        for (name, val) in point.tags {
            row_builder = row_builder.tag(name, val.raw_val);
        }

        for (name, val) in point.fields {
            row_builder = row_builder.field(name, val.raw_val);
        }

        row_builder.finish().map_err(PyTypeError::new_err)
    }
}

impl From<WriteRequest> for rust_model::write::WriteRequest {
    fn from(write_req: WriteRequest) -> Self {
        write_req.builder.build()
    }
}

#[pyclass]
pub struct WriteResponse {
    pub raw_resp: Arc<RustWriteResponse>,
}

#[pymethods]
impl WriteResponse {
    pub fn get_success(&self) -> u32 {
        self.raw_resp.success
    }

    pub fn get_failed(&self) -> u32 {
        self.raw_resp.failed
    }
}
