use std::future::Future;

use storb_protocol::node::NodeUid;

/// Ability to synchronize a node with the subnet's metagraph.
pub trait Synchronizable {
    type Error;

    /// Synchronize the local metagraph with the chain.
    fn sync_metagraph(&mut self) -> impl Future<Output = Result<Vec<NodeUid>, Self::Error>> + Send;
}
