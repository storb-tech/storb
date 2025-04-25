//! Settings for Storb, defined in the settings.toml file.

use config::{Config, ConfigError, File};
use libp2p::Multiaddr;
use serde::Deserialize;

#[allow(unused)]
#[derive(Debug, Deserialize)]
pub struct Subtensor {
    pub network: String,
    pub address: String,
    #[serde(default)]
    pub insecure: bool,
}

#[allow(unused)]
#[derive(Debug, Deserialize)]
pub struct Neuron {
    pub sync_frequency: u64,
}

#[allow(unused)]
#[derive(Debug, Deserialize)]
pub struct Dht {
    pub port: u16,
    #[serde(default)]
    pub no_bootstrap: bool,
    #[serde(default)]
    pub bootstrap_nodes: Option<Vec<Multiaddr>>,
}

#[allow(unused)]
#[derive(Debug, Deserialize)]
pub struct Miner {
    pub store_dir: String,
}

#[allow(unused)]
#[derive(Debug, Deserialize)]
pub struct ValidatorNeuron {
    pub num_concurrent_forwards: u64,
    pub disable_set_weights: bool,
    pub moving_average_alpha: f64,
    pub response_time_alpha: f64,
}

#[allow(unused)]
#[derive(Debug, Deserialize)]
pub struct ValidatorQuery {
    pub batch_size: u64,
    pub num_uids: u16,
    pub timeout: u64,
}

#[allow(unused)]
#[derive(Debug, Deserialize)]
pub struct Validator {
    pub scores_state_file: String,
    pub api_keys_db: String,
    pub neuron: ValidatorNeuron,
    pub query: ValidatorQuery,
}

#[allow(unused)]
#[derive(Debug, Deserialize)]
pub struct Settings {
    pub version: String,
    pub log_level: String, // TODO: Add function to convert str -> enum

    pub netuid: u16,
    pub external_ip: String,
    pub api_port: u16,
    pub quic_port: u16,
    pub post_ip: bool,

    pub wallet_path: String,
    pub wallet_name: String,
    pub hotkey_name: String,

    pub mock: bool,

    pub load_old_nodes: bool,
    pub min_stake_threshold: u64,

    pub db_file: String,
    pub dht_dir: String,
    pub neurons_dir: String,
    pub pem_file: String,

    pub subtensor: Subtensor,
    pub neuron: Neuron,
    pub dht: Dht,

    pub miner: Miner,
    pub validator: Validator,
}

impl Settings {
    /// Load settings and create a new `Settings` instance.
    pub(crate) fn new(config_file: Option<&str>) -> Result<Self, ConfigError> {
        let file: &str = match config_file {
            Some(name) => name,
            None => "settings.toml",
        };

        let s = Config::builder()
            .add_source(File::with_name(file))
            .build()?;

        s.try_deserialize()
    }
}

/// Macro to get a value from CLI args if present, otherwise use the settings value.
///
/// # Example
///
/// ```rust
/// get_config_value(args, "arg_name", String, settings.arg_name);
/// ```
#[macro_export]
macro_rules! get_config_value {
    ($args:expr, $arg_name:expr, $arg_type:ty, $settings:expr) => {
        match $args.try_get_one::<$arg_type>($arg_name) {
            Ok(Some(value)) => value,
            Ok(None) => &$settings,
            Err(err) => {
                tracing::warn!("Failed to load CLI config, loading default settings. Error: {err}");
                &$settings
            }
        }
    };
}
