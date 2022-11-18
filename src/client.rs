// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{fmt::Debug, sync::Arc, time::Duration};

use ceresdb_client_rs::{
    db_client::{Builder as RustBuilder, DbClient, Mode as RustMode},
    model as rust_model, RpcConfig as RustRpcConfig, RpcContext as RustRpcContext,
    RpcOptions as RustRpcOptions,
};
use pyo3::{class::basic::CompareOp, exceptions::PyException, prelude::*};
use pyo3_asyncio::tokio;

use crate::{model, model::WriteResponse};

pub fn register_py_module(m: &PyModule) -> PyResult<()> {
    m.add_class::<RpcContext>()?;
    m.add_class::<Client>()?;
    m.add_class::<Builder>()?;
    m.add_class::<RpcOptions>()?;
    m.add_class::<GrpcConfig>()?;
    m.add_class::<Mode>()?;
    m.add("ModeStandalone", Mode(MODE_STANDALONE))?;
    m.add("ModeCluster", Mode(MODE_CLUSTER))?;

    Ok(())
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct RpcContext {
    pub raw_ctx: RustRpcContext,
}

#[pymethods]
impl RpcContext {
    #[new]
    pub fn new(tenant: String, token: String) -> Self {
        let raw_ctx = RustRpcContext::new(tenant, token);
        Self { raw_ctx }
    }

    pub fn set_timeout_in_millis(&mut self, timeout_millis: u64) {
        let timeout = Duration::from_millis(timeout_millis);
        self.raw_ctx.timeout = Some(timeout);
    }

    pub fn __str__(&self) -> String {
        format!("{:?}", self)
    }
}

#[pyclass]
pub struct Client {
    raw_client: Arc<dyn DbClient>,
}

fn to_py_exception(err: impl Debug) -> PyErr {
    PyException::new_err(format!("{:?}", err))
}

#[pymethods]
impl Client {
    fn query<'p>(
        &self,
        py: Python<'p>,
        ctx: &RpcContext,
        req: &model::QueryRequest,
    ) -> PyResult<&'p PyAny> {
        // TODO(kamille) can avoid cloning?
        let raw_req = rust_model::request::QueryRequest {
            metrics: req.metrics.clone(),
            ql: req.ql.clone(),
        };

        let raw_client = self.raw_client.clone();
        let raw_ctx = ctx.raw_ctx.clone();
        tokio::future_into_py(py, async move {
            let query_resp = raw_client
                .query(&raw_ctx, &raw_req)
                .await
                .map_err(to_py_exception)?;
            model::convert_query_response(query_resp).map_err(to_py_exception)
        })
    }

    fn write<'p>(
        &self,
        py: Python<'p>,
        ctx: &RpcContext,
        req: &model::WriteRequest,
    ) -> PyResult<&'p PyAny> {
        let raw_client = self.raw_client.clone();
        let raw_ctx = ctx.raw_ctx.clone();
        let raw_req: rust_model::write::WriteRequest = (*req).clone().into();
        tokio::future_into_py(py, async move {
            let rust_resp = raw_client
                .write(&raw_ctx, &raw_req)
                .await
                .map_err(to_py_exception)?;
            Ok(WriteResponse {
                raw_resp: Arc::new(rust_resp),
            })
        })
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct GrpcConfig {
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
impl GrpcConfig {
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

#[pyclass]
pub struct Builder {
    raw_builder: RustBuilder,
}

#[pyclass]
#[derive(Clone, Copy, Debug)]
pub struct Mode(u8);

pub const MODE_STANDALONE: u8 = 0;
pub const MODE_CLUSTER: u8 = 1;

impl ToString for Mode {
    fn to_string(&self) -> String {
        let type_desc = match self.0 {
            MODE_STANDALONE => "standalone",
            MODE_CLUSTER => "cluster",
            _ => return format!("Unknown mode:{}", self.0),
        };

        type_desc.to_string()
    }
}

#[pymethods]
impl Mode {
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

impl TryFrom<Mode> for RustMode {
    type Error = PyErr;

    fn try_from(mode: Mode) -> Result<Self, Self::Error> {
        let rust_mode = match mode.0 {
            MODE_STANDALONE => RustMode::Standalone,
            MODE_CLUSTER => RustMode::Cluster,
            _ => {
                return Err(to_py_exception(format!(
                    "invalid mode:{}",
                    mode.to_string(),
                )))
            }
        };

        Ok(rust_mode)
    }
}

#[pymethods]
impl Builder {
    #[new]
    pub fn new(endpoint: String, mode: Mode) -> PyResult<Self> {
        let rust_mode = RustMode::try_from(mode)?;
        let builder = RustBuilder::new(endpoint, rust_mode);

        Ok(Self {
            raw_builder: builder,
        })
    }

    pub fn set_grpc_config(&mut self, conf: GrpcConfig) {
        let thread_num = if conf.thread_num > 0 {
            Some(conf.thread_num as usize)
        } else {
            None
        };
        let raw_grpc_config = RustRpcConfig {
            thread_num,
            max_send_msg_len: conf.max_send_msg_len,
            max_recv_msg_len: conf.max_recv_msg_len,
            keep_alive_interval: Duration::from_millis(conf.keep_alive_interval_ms),
            keep_alive_timeout: Duration::from_millis(conf.keep_alive_timeout_ms),
            keep_alive_while_idle: conf.keep_alive_while_idle,
        };
        self.raw_builder = self.raw_builder.clone().grpc_config(raw_grpc_config);
    }

    pub fn set_rpc_options(&mut self, opts: RpcOptions) {
        let raw_rpc_options = RustRpcOptions {
            write_timeout: Duration::from_millis(opts.write_timeout_ms),
            read_timeout: Duration::from_millis(opts.read_timeout_ms),
            connect_timeout: Duration::from_millis(opts.read_timeout_ms),
        };
        self.raw_builder = self.raw_builder.clone().rpc_opts(raw_rpc_options);
    }

    pub fn build(&self) -> Client {
        let client = self.raw_builder.clone().build();
        Client { raw_client: client }
    }
}
