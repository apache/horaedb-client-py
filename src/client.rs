// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{fmt::Debug, sync::Arc, time::Duration};

use ceresdb_client_rs::{client, model as rust_model, options, DbClient};
use pyo3::{exceptions::PyException, prelude::*};
use pyo3_asyncio::tokio;

use crate::{model, model::WriteResponse};

pub fn register_py_module(m: &PyModule) -> PyResult<()> {
    m.add_class::<RpcContext>()?;
    m.add_class::<Client>()?;
    m.add_class::<Builder>()?;
    m.add_class::<RpcOptions>()?;
    m.add_class::<GrpcConfig>()
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct RpcContext {
    pub raw_ctx: Arc<client::RpcContext>,
}

#[pymethods]
impl RpcContext {
    #[new]
    pub fn new(tenant: String, token: String) -> Self {
        let raw_ctx = client::RpcContext { tenant, token };
        Self {
            raw_ctx: Arc::new(raw_ctx),
        }
    }

    pub fn __str__(&self) -> String {
        format!("{:?}", self)
    }
}

#[pyclass]
pub struct Client {
    raw_client: client::Client,
}

fn to_py_exception(err: impl Debug) -> PyErr {
    PyException::new_err(format!("{:?}", err))
}

#[pymethods]
impl Client {
    #[new]
    fn new(endpoint: String) -> Self {
        Builder::new(endpoint).build()
    }

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
                .query(&*raw_ctx, &raw_req)
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
            let write_result = raw_client
                .write(&*raw_ctx, &raw_req)
                .await
                .map_err(to_py_exception)?;
            Ok(WriteResponse {
                write_result: Arc::new(write_result),
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
    pub keepalive_time_ms: u64,
    #[pyo3(get, set)]
    pub keepalive_timeout_ms: u64,
}

#[pymethods]
impl GrpcConfig {
    #[new]
    pub fn new(
        thread_num: i32,
        max_send_msg_len: i32,
        max_recv_msg_len: i32,
        keepalive_time_ms: u64,
        keepalive_timeout_ms: u64,
    ) -> Self {
        Self {
            thread_num,
            max_send_msg_len,
            max_recv_msg_len,
            keepalive_time_ms,
            keepalive_timeout_ms,
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
}

#[pymethods]
impl RpcOptions {
    #[new]
    pub fn new(write_timeout_ms: u64, read_timeout_ms: u64) -> Self {
        Self {
            write_timeout_ms,
            read_timeout_ms,
        }
    }
}

#[pyclass]
pub struct Builder {
    raw_builder: client::Builder,
}

#[pymethods]
impl Builder {
    #[new]
    pub fn new(endpoint: String) -> Self {
        let builder = client::Builder::new(endpoint);
        Self {
            raw_builder: builder,
        }
    }

    pub fn set_grpc_config(&mut self, conf: GrpcConfig) {
        let thread_num = if conf.thread_num > 0 {
            Some(conf.thread_num as usize)
        } else {
            None
        };
        let raw_grpc_config = options::GrpcConfig {
            thread_num,
            max_send_msg_len: conf.max_send_msg_len,
            max_recv_msg_len: conf.max_recv_msg_len,
            keepalive_time: Duration::from_millis(conf.keepalive_time_ms),
            keepalive_timeout: Duration::from_millis(conf.keepalive_timeout_ms),
        };
        self.raw_builder = self.raw_builder.clone().grpc_config(raw_grpc_config);
    }

    pub fn set_rpc_options(&mut self, opts: RpcOptions) {
        let raw_rpc_options = options::RpcOptions {
            write_timeout: Duration::from_millis(opts.write_timeout_ms),
            read_timeout: Duration::from_millis(opts.read_timeout_ms),
        };
        self.raw_builder = self.raw_builder.clone().rpc_opts(raw_rpc_options);
    }

    pub fn build(&self) -> Client {
        let client = self.raw_builder.clone().build();
        Client { raw_client: client }
    }
}
