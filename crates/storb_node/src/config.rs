use std::path::PathBuf;

use storb_core::version::Version;

#[derive(Clone, Debug)]
pub struct SubtensorConfig {
    pub network: String,
    pub address: String,
    pub insecure: bool,
}

#[derive(Clone, Debug)]
pub struct NeuronConfig {
    pub sync_frequency: u64,
}

#[derive(Clone, Debug)]
pub struct NodeConfig {
    /// Version of Storb.
    pub version: Version,

    /// UID for the subnet.
    pub netuid: u16,
    /// External IP address of the node.
    pub external_ip: String,
    /// HTTP port for the node.
    pub api_port: u16,
    /// QUIC port for the node.
    pub quic_port: Option<u16>,
    /// Whether the node should publish its IP address or not.
    pub post_ip: bool,

    /// Path to the Bittensor wallet file.
    pub wallet_path: PathBuf,
    /// Name of the wallet.
    pub wallet_name: String,
    /// Name of the hotkey associated with the wallet.
    pub hotkey_name: String,

    /// OpenTelemetry API key.
    pub otel_api_key: String,
    /// OpenTelemetry endpoint.
    pub otel_endpoint: String,
    /// OpenTelemetry service name.
    pub otel_service_name: String,

    /// Whether to run the node in mock mode (for testing).
    pub mock: bool,

    /// Path to the neuron state
    pub neurons_dir: PathBuf, // TODO: rename

    /// Subbtensor-specific configuration.
    pub subtensor: SubtensorConfig,

    /// Neuron-specific configuration.
    pub neuron: NeuronConfig,
}
