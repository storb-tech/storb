use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use capnp_rpc::{twoparty, RpcSystem};
use quinn::{Endpoint, ServerConfig as QuinnServerConfig};

use crate::service::Service;

pub trait AsyncRuntime: Send + Sync + 'static + Clone {
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static;
}

#[derive(Clone)]
pub struct TokioRuntime;

impl AsyncRuntime for TokioRuntime {
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(future);
    }
}

#[derive(Clone)]
pub struct ServerOptions {
    pub addr: SocketAddr,
    pub tls: Option<quinn::ServerConfig>,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:5000".parse().unwrap(),
            tls: None,
        }
    }
}

pub struct ServerBuilder<R = TokioRuntime> {
    opts: ServerOptions,
    services: Vec<Arc<dyn Service>>,
    runtime: R,
}

impl ServerBuilder<TokioRuntime> {
    pub fn new() -> Self {
        Self {
            opts: ServerOptions::default(),
            services: Vec::new(),
            runtime: TokioRuntime,
        }
    }
}

impl<R> ServerBuilder<R>
where
    R: AsyncRuntime + Clone,
{
    pub fn bind_addr(mut self, addr: SocketAddr) -> Self {
        self.opts.addr = addr;
        self
    }

    pub fn tls_config(mut self, cfg: Option<quinn::ServerConfig>) -> Self {
        self.opts.tls = cfg;
        self
    }

    pub fn add_service<S: Service>(mut self, svc: S) -> Self {
        self.services.push(Arc::new(svc));
        self
    }

    pub fn with_runtime<NewR: AsyncRuntime>(self, runtime: NewR) -> ServerBuilder<NewR> {
        ServerBuilder {
            opts: self.opts,
            services: self.services,
            runtime,
        }
    }

    pub async fn serve(self) -> anyhow::Result<()> {
        let tls_cfg = if let Some(cfg) = self.opts.tls {
            cfg
        } else {
            let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
            let key = cert.serialize_der()?;
            let cert = cert.serialize_der()?;
            let mut server_cfg = quinn::ServerConfig::with_single_cert(vec![cert], key)?;
            Arc::make_mut(&mut server_cfg.transport).max_concurrent_uni_streams(0_u8.into());
            server_cfg
        };

        let (endpoint, mut incoming) = Endpoint::server(tls_cfg, self.opts.addr)?;
        println!("Listening on {}", self.opts.addr);

        while let Some(conn) = incoming.next().await {
            let services = self.services.clone();
            let runtime = self.runtime.clone();

            runtime.spawn(async move {
                if let Ok(new_conn) = conn.await {
                    println!("Connection from {}", new_conn.remote_address());

                    while let Ok((send, recv)) = new_conn.accept_bi().await {
                        let mut rpc_sys = RpcSystem::new(
                            Box::new(twoparty::VatNetwork::new(
                                recv,
                                send,
                                twoparty::Side::Server,
                                Default::default(),
                            )),
                            None,
                        );

                        for svc in &services {
                            svc.register_methods(&mut rpc_sys);
                        }

                        tokio::task::spawn_local(rpc_sys.map(|_| ()));
                    }
                }
            });
        }

        Ok(())
    }
}
