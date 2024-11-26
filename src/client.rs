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

use std::{fmt::Debug, sync::Arc, time::Duration};

use horaedb_client::{
    db_client::{Builder as RustBuilder, DbClient, Mode as RustMode},
    RpcConfig as RustRpcConfig, RpcContext as RustRpcContext,
};
use pyo3::{exceptions::PyException, prelude::*};
use pyo3_asyncio::tokio;

use crate::{
    model,
    model::{SqlQueryResponse, WriteResponse},
};

pub fn register_py_module(m: &PyModule) -> PyResult<()> {
    m.add_class::<RpcContext>()?;
    m.add_class::<Client>()?;
    m.add_class::<Builder>()?;
    m.add_class::<RpcConfig>()?;
    m.add_class::<Mode>()?;
    m.add_class::<Authorization>()?;

    Ok(())
}

/// The context used for a specific rpc call, and it will overwrite the default
/// options.
#[pyclass]
#[derive(Clone, Debug, Default)]
pub struct RpcContext {
    #[pyo3(get, set)]
    database: Option<String>,
    #[pyo3(get, set)]
    timeout_ms: Option<u64>,
}

#[pymethods]
impl RpcContext {
    #[new]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn __str__(&self) -> String {
        format!("{self:?}")
    }
}

impl From<RpcContext> for RustRpcContext {
    fn from(ctx: RpcContext) -> Self {
        Self {
            database: ctx.database,
            timeout: ctx.timeout_ms.map(Duration::from_millis),
        }
    }
}

/// The client for HoraeDB.
///
/// It is just a wrapper on the rust client, and it is thread-safe.
#[pyclass]
pub struct Client {
    rust_client: Arc<dyn DbClient>,
}

fn to_py_exception(err: impl Debug) -> PyErr {
    PyException::new_err(format!("{err:?}"))
}

#[pymethods]
impl Client {
    fn write<'p>(
        &self,
        py: Python<'p>,
        ctx: RpcContext,
        req: model::WriteRequest,
    ) -> PyResult<&'p PyAny> {
        let rust_client = self.rust_client.clone();

        tokio::future_into_py(py, async move {
            let rust_req = req.as_ref();
            let rust_ctx = ctx.into();
            let rust_resp = rust_client
                .write(&rust_ctx, rust_req)
                .await
                .map_err(to_py_exception)?;
            Ok(WriteResponse::from(rust_resp))
        })
    }

    fn sql_query<'p>(
        &self,
        py: Python<'p>,
        ctx: RpcContext,
        req: model::SqlQueryRequest,
    ) -> PyResult<&'p PyAny> {
        let rust_client = self.rust_client.clone();

        tokio::future_into_py(py, async move {
            let rust_req = req.as_ref();
            let rust_ctx = ctx.into();
            let query_resp = rust_client
                .sql_query(&rust_ctx, rust_req)
                .await
                .map_err(to_py_exception)?;
            Ok(SqlQueryResponse::from(query_resp))
        })
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct RpcConfig {
    /// Set the thread num as the cpu cores number if the number is not
    /// positive.
    #[pyo3(get, set)]
    pub thread_num: i32,
    /// -1 means unlimited
    #[pyo3(get, set)]
    pub max_send_msg_len: i32,
    /// -1 means unlimited
    #[pyo3(get, set)]
    pub max_recv_msg_len: i32,
    #[pyo3(get, set)]
    pub keep_alive_interval_ms: u64,
    #[pyo3(get, set)]
    pub keep_alive_timeout_ms: u64,
    #[pyo3(get, set)]
    pub keep_alive_while_idle: bool,
    #[pyo3(get, set)]
    pub default_write_timeout_ms: u64,
    #[pyo3(get, set)]
    pub default_sql_query_timeout_ms: u64,
    #[pyo3(get, set)]
    pub connect_timeout_ms: u64,
}

#[pymethods]
impl RpcConfig {
    #[new]
    pub fn new() -> Self {
        let default_rust_config = RustRpcConfig::default();
        Self::from(default_rust_config)
    }
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl From<RpcConfig> for RustRpcConfig {
    fn from(config: RpcConfig) -> Self {
        let thread_num = if config.thread_num > 0 {
            Some(config.thread_num as usize)
        } else {
            None
        };
        Self {
            thread_num,
            max_send_msg_len: config.max_send_msg_len,
            max_recv_msg_len: config.max_recv_msg_len,
            keep_alive_interval: Duration::from_millis(config.keep_alive_interval_ms),
            keep_alive_timeout: Duration::from_millis(config.keep_alive_timeout_ms),
            keep_alive_while_idle: config.keep_alive_while_idle,
            default_write_timeout: Duration::from_millis(config.default_write_timeout_ms),
            default_sql_query_timeout: Duration::from_millis(config.default_sql_query_timeout_ms),
            connect_timeout: Duration::from_millis(config.connect_timeout_ms),
        }
    }
}

impl From<RustRpcConfig> for RpcConfig {
    fn from(config: RustRpcConfig) -> Self {
        let thread_num = config.thread_num.unwrap_or(0) as i32;
        Self {
            thread_num,
            max_send_msg_len: config.max_send_msg_len,
            max_recv_msg_len: config.max_recv_msg_len,
            keep_alive_interval_ms: config.keep_alive_interval.as_millis() as u64,
            keep_alive_timeout_ms: config.keep_alive_timeout.as_millis() as u64,
            keep_alive_while_idle: config.keep_alive_while_idle,
            default_write_timeout_ms: config.default_write_timeout.as_millis() as u64,
            default_sql_query_timeout_ms: config.default_sql_query_timeout.as_millis() as u64,
            connect_timeout_ms: config.connect_timeout.as_millis() as u64,
        }
    }
}

/// A builder for the client.
#[pyclass]
pub struct Builder {
    /// The builder is used to build the client.
    ///
    /// The option is a workaround for using builder pattern of [`RustBuilder`],
    /// and it is ensured to be `Some`.
    rust_builder: Option<RustBuilder>,
}

/// The mode of the communication between client and server.
///
/// In `Direct` mode, request will be sent to corresponding endpoint
/// directly(maybe need to get the target endpoint by route request first).
/// In `Proxy` mode, request will be sent to proxy server responsible for
/// forwarding the request.
#[pyclass]
#[derive(Debug, Clone)]
pub enum Mode {
    Direct,
    Proxy,
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct Authorization {
    username: String,
    password: String,
}

#[pymethods]
impl Authorization {
    #[new]
    pub fn new(username: String, password: String) -> Self {
        Self { username, password }
    }
}

impl From<Authorization> for horaedb_client::Authorization {
    fn from(auth: Authorization) -> Self {
        Self {
            username: auth.username,
            password: auth.password,
        }
    }
}

#[pymethods]
impl Builder {
    #[new]
    pub fn new(endpoint: String, mode: Mode) -> Self {
        let rust_mode = match mode {
            Mode::Direct => RustMode::Direct,
            Mode::Proxy => RustMode::Proxy,
        };

        let builder = RustBuilder::new(endpoint, rust_mode);

        Self {
            rust_builder: Some(builder),
        }
    }

    pub fn set_rpc_config(&mut self, conf: RpcConfig) {
        let builder = self.rust_builder.take().unwrap().rpc_config(conf.into());
        self.rust_builder = Some(builder);
    }

    pub fn set_default_database(&mut self, db: String) {
        let builder = self.rust_builder.take().unwrap().default_database(db);
        self.rust_builder = Some(builder);
    }

    pub fn set_authorization(&mut self, auth: Authorization) {
        let builder = self.rust_builder.take().unwrap().authorization(auth.into());
        self.rust_builder = Some(builder);
    }

    pub fn build(&mut self) -> Client {
        let client = self.rust_builder.take().unwrap().build();
        Client {
            rust_client: client,
        }
    }
}
