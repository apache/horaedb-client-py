// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Read/Write request and response, and useful tools for them.

use std::sync::Arc;

use horaedb_client::model::{
    sql_query::{
        row::{Column as RustColumn, Row as RustRow},
        Request as RustSqlQueryRequest, Response as RustSqlQueryResponse,
    },
    value::{DataType as RustDataType, TimestampMs, Value as RustValue},
    write::{
        point::{Point as RustPoint, PointBuilder as RustPointBuilder},
        Request as RustWriteRequest, Response as RustWriteResponse,
    },
};
use pyo3::{exceptions::PyTypeError, prelude::*};

pub fn register_py_module(m: &PyModule) -> PyResult<()> {
    m.add_class::<SqlQueryRequest>()?;
    m.add_class::<SqlQueryResponse>()?;
    m.add_class::<DataType>()?;
    m.add_class::<Column>()?;
    m.add_class::<ColumnIter>()?;
    m.add_class::<Row>()?;
    m.add_class::<RowIter>()?;
    m.add_class::<Value>()?;
    m.add_class::<ValueBuilder>()?;
    m.add_class::<PointBuilder>()?;
    m.add_class::<Point>()?;
    m.add_class::<WriteRequest>()?;
    m.add_class::<WriteResponse>()?;

    Ok(())
}

/// A sql query request.
#[pyclass]
#[derive(Clone, Debug)]
pub struct SqlQueryRequest {
    rust_req: RustSqlQueryRequest,
}

#[pymethods]
impl SqlQueryRequest {
    #[new]
    pub fn new(tables: Vec<String>, sql: String) -> Self {
        let rust_req = RustSqlQueryRequest { tables, sql };
        Self { rust_req }
    }

    pub fn __str__(&self) -> String {
        format!("{self:?}")
    }
}

impl From<SqlQueryRequest> for RustSqlQueryRequest {
    fn from(req: SqlQueryRequest) -> Self {
        req.rust_req
    }
}

impl AsRef<RustSqlQueryRequest> for SqlQueryRequest {
    fn as_ref(&self) -> &RustSqlQueryRequest {
        &self.rust_req
    }
}

/// [SqlQueryResponse] is the response of a sql query.
#[pyclass]
#[derive(Clone, Debug)]
pub struct SqlQueryResponse {
    rust_rows: Arc<Vec<RustRow>>,
    #[pyo3(get)]
    affected_rows: u32,
}

#[pymethods]
impl SqlQueryResponse {
    pub fn num_rows(&self) -> usize {
        self.rust_rows.len()
    }

    pub fn row_by_idx(&self, row_idx: usize) -> Option<Row> {
        if self.rust_rows.len() > row_idx {
            Some(Row {
                rust_rows: self.rust_rows.clone(),
                row_idx,
            })
        } else {
            None
        }
    }

    pub fn iter_rows(&self) -> RowIter {
        RowIter {
            rust_rows: self.rust_rows.clone(),
            next_row_idx: 0,
        }
    }

    pub fn __str__(&self) -> String {
        format!("{self:?}")
    }
}

#[pyclass]
#[derive(Clone)]
pub struct RowIter {
    rust_rows: Arc<Vec<RustRow>>,
    next_row_idx: usize,
}

#[pymethods]
impl RowIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<Row> {
        if slf.rust_rows.len() > slf.next_row_idx {
            let row_idx = slf.next_row_idx;
            slf.next_row_idx += 1;
            Some(Row {
                rust_rows: slf.rust_rows.clone(),
                row_idx,
            })
        } else {
            None
        }
    }

    pub fn __str__(&self) -> String {
        format!(
            "total_rows:{}, next_row_idx:{}",
            self.rust_rows.len(),
            self.next_row_idx
        )
    }
}

impl From<RustSqlQueryResponse> for SqlQueryResponse {
    fn from(query_resp: RustSqlQueryResponse) -> Self {
        SqlQueryResponse {
            rust_rows: Arc::new(query_resp.rows),
            affected_rows: query_resp.affected_rows,
        }
    }
}

/// The data type definitions for read/write protocol.
#[pyclass]
#[derive(Clone, Copy, Debug)]
pub enum DataType {
    Null = 0,
    Timestamp,
    Double,
    Float,
    Varbinary,
    String,
    UInt64,
    UInt32,
    UInt16,
    UInt8,
    Int64,
    Int32,
    Int16,
    Int8,
    Boolean,
}

impl From<RustDataType> for DataType {
    fn from(typ: RustDataType) -> Self {
        match typ {
            RustDataType::Null => DataType::Null,
            RustDataType::Timestamp => DataType::Timestamp,
            RustDataType::Double => DataType::Double,
            RustDataType::Float => DataType::Float,
            RustDataType::Varbinary => DataType::Varbinary,
            RustDataType::String => DataType::String,
            RustDataType::UInt64 => DataType::UInt64,
            RustDataType::UInt32 => DataType::UInt32,
            RustDataType::UInt16 => DataType::UInt16,
            RustDataType::UInt8 => DataType::UInt8,
            RustDataType::Int64 => DataType::Int64,
            RustDataType::Int32 => DataType::Int32,
            RustDataType::Int16 => DataType::Int16,
            RustDataType::Int8 => DataType::Int8,
            RustDataType::Boolean => DataType::Boolean,
        }
    }
}

/// A column of data returned from a sql query.
#[pyclass]
#[derive(Clone, Debug)]
pub struct Column {
    row_idx: usize,
    col_idx: usize,
    rust_rows: Arc<Vec<RustRow>>,
}

impl Column {
    fn get_rust_col(&self) -> &RustColumn {
        &self.rust_rows[self.row_idx].columns()[self.col_idx]
    }
}

#[pymethods]
impl Column {
    pub fn value(&self, py: Python<'_>) -> PyObject {
        match self.get_rust_col().value() {
            RustValue::Null => py.None(),
            RustValue::Timestamp(v) => (*v).to_object(py),
            RustValue::Double(v) => (*v).to_object(py),
            RustValue::Float(v) => (*v).to_object(py),
            RustValue::Varbinary(v) => v.as_slice().to_object(py),
            RustValue::String(v) => v.as_str().to_object(py),
            RustValue::UInt64(v) => (*v).to_object(py),
            RustValue::UInt32(v) => (*v).to_object(py),
            RustValue::UInt16(v) => (*v).to_object(py),
            RustValue::UInt8(v) => (*v).to_object(py),
            RustValue::Int64(v) => (*v).to_object(py),
            RustValue::Int32(v) => (*v).to_object(py),
            RustValue::Int16(v) => (*v).to_object(py),
            RustValue::Int8(v) => (*v).to_object(py),
            RustValue::Boolean(v) => (*v).to_object(py),
        }
    }

    pub fn data_type(&self) -> DataType {
        self.get_rust_col().value().data_type().into()
    }

    pub fn name(&self) -> &str {
        self.get_rust_col().name()
    }

    pub fn __str__(&self) -> String {
        let rust_col = self.get_rust_col();
        format!("{rust_col:?}")
    }
}

#[pyclass]
#[derive(Clone)]
pub struct ColumnIter {
    rust_rows: Arc<Vec<RustRow>>,
    row_idx: usize,
    next_col_idx: usize,
}

#[pymethods]
impl ColumnIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<Column> {
        // The row idx should be ensured in range.
        let rust_row = &slf.rust_rows[slf.row_idx];
        if slf.next_col_idx < rust_row.columns().len() {
            let col_idx = slf.next_col_idx;
            slf.next_col_idx += 1;
            Some(Column {
                rust_rows: slf.rust_rows.clone(),
                row_idx: slf.row_idx,
                col_idx,
            })
        } else {
            None
        }
    }
}

/// A row of data returned from a sql query.
#[pyclass]
#[derive(Debug, Clone)]
pub struct Row {
    row_idx: usize,
    rust_rows: Arc<Vec<RustRow>>,
}

#[pymethods]
impl Row {
    pub fn column(&self, col_name: &str) -> Option<Column> {
        let row = &self.rust_rows[self.row_idx];
        let col_idx = row.columns().iter().position(|c| c.name() == col_name);
        if let Some(col_idx) = col_idx {
            let col = Column {
                row_idx: self.row_idx,
                col_idx,
                rust_rows: self.rust_rows.clone(),
            };
            Some(col)
        } else {
            None
        }
    }

    pub fn column_by_idx(&self, col_idx: usize) -> Option<Column> {
        let row = &self.rust_rows[self.row_idx];

        if col_idx >= row.columns().len() {
            None
        } else {
            let col = Column {
                row_idx: self.row_idx,
                col_idx,
                rust_rows: self.rust_rows.clone(),
            };
            Some(col)
        }
    }

    pub fn num_cols(&self) -> usize {
        self.rust_rows[self.row_idx].columns().len()
    }

    pub fn iter_columns(&self) -> ColumnIter {
        ColumnIter {
            rust_rows: self.rust_rows.clone(),
            row_idx: self.row_idx,
            next_col_idx: 0,
        }
    }

    pub fn __str__(&self) -> String {
        let rust_row = &self.rust_rows[self.row_idx];
        format!("{rust_row:?}")
    }
}

/// [Value] is a wrapper of [RustValue], used for writing.
#[pyclass]
#[derive(Clone, Debug)]
pub struct Value {
    raw_val: RustValue,
}

/// Builder for a [Value].
#[pyclass]
#[derive(Clone, Debug, Default)]
pub struct ValueBuilder;

#[pymethods]
impl ValueBuilder {
    #[new]
    pub fn new() -> Self {
        Self
    }

    pub fn null(&self) -> Value {
        Value {
            raw_val: RustValue::Null,
        }
    }

    pub fn timestamp(&self, timestamp_mills: i64) -> Value {
        Value {
            raw_val: RustValue::Timestamp(timestamp_mills),
        }
    }

    pub fn double(&self, val: f64) -> Value {
        Value {
            raw_val: RustValue::Double(val),
        }
    }

    pub fn float(&self, val: f32) -> Value {
        Value {
            raw_val: RustValue::Float(val),
        }
    }

    pub fn string(&self, val: String) -> Value {
        Value {
            raw_val: RustValue::String(val),
        }
    }

    pub fn varbinary(&self, val: Vec<u8>) -> Value {
        Value {
            raw_val: RustValue::Varbinary(val),
        }
    }

    pub fn uint64(&self, val: u64) -> Value {
        Value {
            raw_val: RustValue::UInt64(val),
        }
    }

    pub fn uint32(&self, val: u32) -> Value {
        Value {
            raw_val: RustValue::UInt32(val),
        }
    }

    pub fn uint16(&self, val: i16) -> Value {
        Value {
            raw_val: RustValue::Int16(val),
        }
    }

    pub fn uint8(&self, val: u8) -> Value {
        Value {
            raw_val: RustValue::UInt8(val),
        }
    }

    pub fn int64(&self, val: i64) -> Value {
        Value {
            raw_val: RustValue::Int64(val),
        }
    }

    pub fn int32(&self, val: i32) -> Value {
        Value {
            raw_val: RustValue::Int32(val),
        }
    }

    pub fn int16(&self, val: u16) -> Value {
        Value {
            raw_val: RustValue::UInt16(val),
        }
    }

    pub fn int8(&self, val: i8) -> Value {
        Value {
            raw_val: RustValue::Int8(val),
        }
    }

    pub fn bool(&self, val: bool) -> Value {
        Value {
            raw_val: RustValue::Boolean(val),
        }
    }
}

impl From<Value> for RustValue {
    fn from(val: Value) -> Self {
        val.raw_val
    }
}

/// [Point] represents one data row needed to write.
#[pyclass]
#[derive(Clone, Debug)]
pub struct Point {
    rust_point: RustPoint,
}

/// The builder for [Point].
#[pyclass]
pub struct PointBuilder {
    /// The underlying builder defined in rust.
    ///
    /// The option is a workaround to use the builder pattern of the
    /// `RustPointBuilder`, and it is ensured to be `Some` all the time.
    rust_builder: Option<RustPointBuilder>,
}

#[pymethods]
impl PointBuilder {
    #[new]
    pub fn new(table: String) -> Self {
        Self {
            rust_builder: Some(RustPointBuilder::new(table)),
        }
    }

    pub fn set_table(&mut self, table: String) {
        let builder = self.rust_builder.take().unwrap().table(table);
        self.rust_builder = Some(builder);
    }

    pub fn set_timestamp(&mut self, timestamp: TimestampMs) {
        let builder = self.rust_builder.take().unwrap().timestamp(timestamp);
        self.rust_builder = Some(builder);
    }

    pub fn set_tag(&mut self, name: String, val: Value) {
        let builder = self.rust_builder.take().unwrap().tag(name, val.raw_val);
        self.rust_builder = Some(builder);
    }

    pub fn set_field(&mut self, name: String, val: Value) {
        let builder = self.rust_builder.take().unwrap().field(name, val.raw_val);
        self.rust_builder = Some(builder);
    }

    pub fn build(&mut self) -> PyResult<Point> {
        let rust_point = self
            .rust_builder
            .take()
            .unwrap()
            .build()
            .map_err(PyTypeError::new_err)?;

        Ok(Point { rust_point })
    }
}

/// A wrapper for `WriteRequestBuilder`.
#[pyclass]
#[derive(Clone, Debug, Default)]
pub struct WriteRequest {
    rust_request: RustWriteRequest,
}

#[pymethods]
impl WriteRequest {
    #[new]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_point(&mut self, point: Point) {
        self.rust_request.add_point(point.rust_point);
    }

    pub fn add_points(&mut self, points: Vec<Point>) {
        for point in points {
            self.add_point(point);
        }
    }

    pub fn __str__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.rust_request))
    }
}

impl From<WriteRequest> for RustWriteRequest {
    fn from(write_req: WriteRequest) -> Self {
        write_req.rust_request
    }
}

impl AsRef<RustWriteRequest> for WriteRequest {
    fn as_ref(&self) -> &RustWriteRequest {
        &self.rust_request
    }
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct WriteResponse {
    rust_response: RustWriteResponse,
}

#[pymethods]
impl WriteResponse {
    pub fn get_success(&self) -> u32 {
        self.rust_response.success
    }

    pub fn get_failed(&self) -> u32 {
        self.rust_response.failed
    }

    pub fn __str__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.rust_response))
    }
}

impl From<RustWriteResponse> for WriteResponse {
    fn from(resp: RustWriteResponse) -> Self {
        Self {
            rust_response: resp,
        }
    }
}
