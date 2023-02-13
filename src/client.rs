// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{fmt::Debug, sync::Arc, time::Duration};

use ceresdb_client_rs::{
    db_client::{Builder as RustBuilder, DbClient, Mode as RustMode},
    RpcConfig as RustRpcConfig, RpcContext as RustRpcContext, RpcOptions as RustRpcOptions,
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
    m.add_class::<RpcOptions>()?;
    m.add_class::<RpcConfig>()?;
    m.add_class::<Mode>()?;

    Ok(())
}

#[pyclass]
#[derive(Clone, Debug, Default)]
pub struct RpcContext {
    rust_ctx: RustRpcContext,
}

#[pymethods]
impl RpcContext {
    #[new]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_database(&mut self, database: String) {
        self.rust_ctx.database = Some(database);
    }

    pub fn set_timeout_in_millis(&mut self, timeout_millis: u64) {
        let timeout = Duration::from_millis(timeout_millis);
        self.rust_ctx.timeout = Some(timeout);
    }

    pub fn __str__(&self) -> String {
        format!("{:?}", self)
    }
}

impl AsRef<RustRpcContext> for RpcContext {
    fn as_ref(&self) -> &RustRpcContext {
        &self.rust_ctx
    }
}

#[pyclass]
pub struct Client {
    rust_client: Arc<dyn DbClient>,
}

fn to_py_exception(err: impl Debug) -> PyErr {
    PyException::new_err(format!("{:?}", err))
}

#[pymethods]
impl Client {
    fn query<'p>(
        &self,
        py: Python<'p>,
        ctx: RpcContext,
        req: model::SqlQueryRequest,
    ) -> PyResult<&'p PyAny> {
        let rust_client = self.rust_client.clone();

        tokio::future_into_py(py, async move {
            let rust_req = req.as_ref();
            let rust_ctx = ctx.as_ref();
            let query_resp = rust_client
                .sql_query(rust_ctx, rust_req)
                .await
                .map_err(to_py_exception)?;
            Ok(SqlQueryResponse::from(query_resp))
        })
    }

    fn write<'p>(
        &self,
        py: Python<'p>,
        ctx: RpcContext,
        req: model::WriteRequest,
    ) -> PyResult<&'p PyAny> {
        let rust_client = self.rust_client.clone();

        tokio::future_into_py(py, async move {
            let rust_ctx = ctx.as_ref();
            let rust_req = req.as_ref();
            let rust_resp = rust_client
                .write(rust_ctx, rust_req)
                .await
                .map_err(to_py_exception)?;
            Ok(WriteResponse::from(rust_resp))
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
}

#[pymethods]
impl RpcConfig {
    #[new]
    pub fn new(
        thread_num: i32,
        max_send_msg_len: i32,
        max_recv_msg_len: i32,
        keep_alive_interval_ms: u64,
        keep_alive_timeout_ms: u64,
        keep_alive_while_idle: bool,
    ) -> Self {
        Self {
            thread_num,
            max_send_msg_len,
            max_recv_msg_len,
            keep_alive_interval_ms,
            keep_alive_timeout_ms,
            keep_alive_while_idle,
        }
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
        }
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct RpcOptions {
    #[pyo3(get, set)]
    pub write_timeout_ms: u64,
    #[pyo3(get, set)]
    pub read_timeout_ms: u64,
    #[pyo3(get, set)]
    pub connect_timeout_ms: u64,
}

#[pymethods]
impl RpcOptions {
    #[new]
    pub fn new(write_timeout_ms: u64, read_timeout_ms: u64, connect_timeout_ms: u64) -> Self {
        Self {
            write_timeout_ms,
            read_timeout_ms,
            connect_timeout_ms,
        }
    }
}

impl From<RpcOptions> for RustRpcOptions {
    fn from(options: RpcOptions) -> Self {
        Self {
            write_timeout: Duration::from_millis(options.write_timeout_ms),
            read_timeout: Duration::from_millis(options.read_timeout_ms),
            connect_timeout: Duration::from_millis(options.connect_timeout_ms),
        }
    }
}

#[pyclass]
pub struct Builder {
    /// The builder is used to build the client.
    ///
    /// The option is a workaround for using builder pattern, and it is ensured
    /// to be `Some`.
    rust_builder: Option<RustBuilder>,
}

#[pyclass]
#[derive(Debug, Clone)]
pub enum Mode {
    Direct,
    Proxy,
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

    pub fn rpc_config(&mut self, conf: RpcConfig) -> Self {
        let builder = self.rust_builder.take().unwrap().grpc_config(conf.into());

        Self {
            rust_builder: Some(builder),
        }
    }

    pub fn rpc_options(&mut self, opts: RpcOptions) -> Self {
        let builder = self.rust_builder.take().unwrap().rpc_opts(opts.into());

        Self {
            rust_builder: Some(builder),
        }
    }

    pub fn default_database(&mut self, db: String) -> Self {
        let builder = self.rust_builder.take().unwrap().default_database(db);

        Self {
            rust_builder: Some(builder),
        }
    }

    pub fn build(&mut self) -> Client {
        let client = self.rust_builder.take().unwrap().build();
        Client {
            rust_client: client,
        }
    }
}
