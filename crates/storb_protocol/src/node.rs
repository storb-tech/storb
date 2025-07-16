//! Node definitions

use crabtensor::api::runtime_types::pallet_subtensor::rpc_info::neuron_info::NeuronInfoLite;
use crabtensor::AccountId;
use multiaddr::Multiaddr;
use serde::{Deserialize, Serialize};

/// Unique identifier for a node on the subnet.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct NodeUid(pub u16);

impl From<u16> for NodeUid {
    fn from(uid: u16) -> Self {
        NodeUid(uid)
    }
}

impl From<NodeUid> for u16 {
    fn from(value: NodeUid) -> Self {
        value.0
    }
}

/// Information about a remote node.
#[derive(Clone, Debug)]
pub struct NodeInfo {
    /// Information about the node (neuron) from the chain.
    pub info: NeuronInfoLite<AccountId>,

    /// The node's QUIC address.
    pub quic_address: Option<Multiaddr>,
}

/// Information about the local node, retrieved from the chain.
#[derive(Clone, Serialize, Deserialize)]
pub struct LocalNodeInfo {
    /// The node's unique identifier.
    pub uid: Option<NodeUid>,

    /// The node's QUIC address.
    pub quic_address: Option<Multiaddr>,
}
