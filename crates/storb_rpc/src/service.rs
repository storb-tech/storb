use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use capnp_rpc::twoparty::{Side, VatNetwork};
use capnp_rpc::RpcSystem;
use quinn::{RecvStream, SendStream};

pub struct MetadataMap(pub HashMap<String, String>);

impl MetadataMap {
    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.0.keys()
    }
}

pub struct RequestContext {
    pub peer_addr: SocketAddr,
    pub metadata: MetadataMap,
}

impl fmt::Debug for RequestContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestContext")
            .field("peer_addr", &self.peer_addr)
            .field("metadata_keys", &self.metadata.keys().collect::<Vec<_>>())
            .finish()
    }
}

pub trait Middleware: Send + Sync + 'static {
    fn handle(
        &self,
        ctx: RequestContext,
        next: Next,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

impl fmt::Debug for dyn Middleware {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Middleware")
    }
}

pub struct Next<'a> {
    idx: usize,
    middlewares: &'a [Arc<dyn Middleware>],
    service_impl: &'a dyn GeneratedCapnpService,
    rpc_system: &'a mut RpcSystem<VatNetwork<RecvStream, SendStream>>,
}

impl<'a> Next<'a> {
    pub async fn run(mut self, ctx: RequestContext) {
        if self.idx < self.middlewares.len() {
            let mw = &self.middlewares[self.idx];
            self.idx += 1;
            mw.handle(ctx, self).await;
        } else {
            self.service_impl.register_methods_impl(self.rpc_system);
        }
    }
}

pub trait Service: Send + Sync + 'static {
    /// Register all RPC endpoints on the given system.
    fn register_methods(&self, rpc_system: &mut RpcSystem<VatNetwork<RecvStream, SendStream>>);

    /// A stable name for logging
    fn name(&self) -> &'static str;

    /// Health check for graceful shutdown and monitoring.
    fn is_healthy(&self) -> bool {
        true
    }
}

pub struct ServiceBuilder {
    interceptors: Vec<Arc<dyn Interceptor>>,
}

pub trait Interceptor: Send + Sync + 'static {
    fn intercept(&self, ctx: &mut RequestContext);
}

impl ServiceBuilder {
    pub fn new() -> Self {
        ServiceBuilder {
            interceptors: Vec::new(),
        }
    }

    pub fn add_interceptor<I>(mut self, interceptor: I) -> Self
    where
        I: Interceptor,
    {
        self.interceptors.push(Arc::new(interceptor));
        self
    }

    pub fn wrap<T>(self, impl_obj: T) -> CapnpServiceWrapper<T>
    where
        T: GeneratedCapnpService,
    {
        CapnpServiceWrapper {
            inner: Arc::new(impl_obj),
            interceptors: self.interceptors,
        }
    }
}

pub trait GeneratedCapnpService: Send + Sync + 'static {
    fn register_methods_impl(&self, rpc_system: &mut RpcSystem<VatNetwork<RecvStream, SendStream>>);

    fn service_name() -> &'static str
    where
        Self: Sized;

    fn is_healthy(&self) -> bool {
        true
    }
}

pub struct CapnpServiceWrapper<T>
where
    T: GeneratedCapnpService,
{
    inner: Arc<T>,
    interceptors: Vec<Arc<dyn Interceptor>>,
}

impl<T> Service for CapnpServiceWrapper<T>
where
    T: GeneratedCapnpService,
{
    fn register_methods(&self, rpc_system: &mut RpcSystem<VatNetwork<RecvStream, SendStream>>) {
        self.inner.register_methods_impl(rpc_system);
    }

    fn name(&self) -> &'static str {
        T::service_name()
    }

    fn is_healthy(&self) -> bool {
        self.inner.is_healthy()
    }
}
