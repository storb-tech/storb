//! Contains arguments that are used by multiple subcommands, e.g.
//! `miner` and `validator`.

use std::str::FromStr;

use crate::{config::Settings, get_config_value};
use anyhow::Result;
use base::version::Version;
use base::{BaseNeuronConfig, DhtConfig, NeuronConfig, SubtensorConfig};
use clap::{value_parser, Arg, ArgAction, ArgMatches};
use expanduser::expanduser;
use libp2p::Multiaddr;

pub fn common_args() -> Vec<Arg> {
    vec![
        Arg::new("netuid")
            .long("netuid")
            .value_name("id")
            .value_parser(value_parser!(u16))
            .help("Subnet netuid")
            .action(ArgAction::Set)
            .global(true),
        Arg::new("external_ip")
            .long("external-ip")
            .value_name("ip")
            .help("External IP")
            .action(ArgAction::Set)
            .global(true),
        Arg::new("api_port")
            .long("api-port")
            .value_name("port")
            .value_parser(value_parser!(u16))
            .help("API port for the node")
            .action(ArgAction::Set)
            .global(true),
        Arg::new("quic_port")
            .long("quic-port")
            .value_name("port")
            .value_parser(value_parser!(u16))
            .help("QUIC port for the node")
            .action(ArgAction::Set)
            .global(true),
        Arg::new("post_ip")
            .long("post-ip")
            .help("Whether to post the IP to the chain or not")
            .action(ArgAction::SetTrue)
            .global(true),
        Arg::new("wallet_path")
            .long("wallet-path")
            .value_name("wallet")
            .help("Path of wallets")
            .action(ArgAction::Set)
            .global(true),
        Arg::new("wallet_name")
            .long("wallet-name")
            .value_name("wallet")
            .help("Name of the Bittensor wallet")
            .action(ArgAction::Set)
            .global(true),
        Arg::new("hotkey_name")
            .long("hotkey-name")
            .value_name("hotkey")
            .help("Hotkey associated with the wallet")
            .action(ArgAction::Set)
            .global(true),
        Arg::new("mock")
            .long("mock")
            .help("Mock for testing")
            .action(ArgAction::SetTrue)
            .global(true),
        Arg::new("load_old_nodes")
            .long("load-old-nodes")
            .help("Load old nodes")
            .action(ArgAction::SetTrue)
            .global(true),
        Arg::new("min_stake_threshold")
            .long("min-stake-threshold")
            .value_name("threshold")
            .value_parser(value_parser!(u64))
            .help("Minimum stake threshold")
            .action(ArgAction::Set)
            .global(true),
        Arg::new("log_level")
            .long("log-level")
            .value_name("level")
            .help("Set the log level")
            .action(ArgAction::Set)
            .global(true),
        Arg::new("db_file")
            .long("db-file")
            .value_name("path")
            .help("Path to the database file")
            .action(ArgAction::Set)
            .global(true),
        Arg::new("dht_dir")
            .long("dht-dir")
            .value_name("path")
            .help("Path to the DHT RocksDB folder")
            .action(ArgAction::Set)
            .global(true),
        Arg::new("pem_file")
            .long("pem-file")
            .value_name("path")
            .help("Path to the PEM file")
            .action(ArgAction::Set)
            .global(true),
        // Subtensor
        Arg::new("subtensor.network")
            .long("subtensor.network")
            .value_name("network")
            .help("Subtensor network name")
            .action(ArgAction::Set)
            .global(true),
        Arg::new("subtensor.address")
            .long("subtensor.address")
            .value_name("address")
            .help("Subtensor network address")
            .action(ArgAction::Set)
            .global(true),
        Arg::new("subtensor.insecure")
            .long("subtensor.insecure")
            .help("Enable insecure connection to subtensor")
            .action(ArgAction::SetTrue)
            .global(true),
        // Neuron
        Arg::new("neuron.sync_frequency")
            .long("neuron.sync-frequency")
            .value_name("frequency")
            .value_parser(value_parser!(u64))
            .help("The default sync frequency for nodes")
            .action(ArgAction::Set)
            .global(true),
        // DHT
        Arg::new("dht.port")
            .long("dht.port")
            .value_name("port")
            .value_parser(value_parser!(u16))
            .help("DHT port")
            .action(ArgAction::Set)
            .global(true),
        Arg::new("dht.bootstrap_ip")
            .long("dht.bootstrap-ip")
            .value_name("ip")
            .help("Bootstrap node IPv4 address for the DHT")
            .action(ArgAction::Set)
            .global(true),
        Arg::new("dht.bootstrap_port")
            .long("dht.bootstrap-port")
            .value_name("port")
            .value_parser(value_parser!(u16))
            .help("Bootstrap node port for the DHT")
            .action(ArgAction::Set)
            .global(true),
    ]
}

pub fn get_neuron_config(args: &ArgMatches, settings: &Settings) -> Result<BaseNeuronConfig> {
    let api_port = *get_config_value!(args, "api_port", u16, settings.api_port);
    let quic_port = *get_config_value!(args, "quic_port", u16, settings.quic_port);

    let subtensor_config = SubtensorConfig {
        network: get_config_value!(
            args,
            "subtensor.network",
            String,
            &settings.subtensor.network
        )
        .to_string(),
        address: get_config_value!(
            args,
            "subtensor.address",
            String,
            &settings.subtensor.address
        )
        .to_string(),
        insecure: *get_config_value!(
            args,
            "subtensor.insecure",
            bool,
            &settings.subtensor.insecure
        ),
    };

    let neuron_config = NeuronConfig {
        sync_frequency: *get_config_value!(
            args,
            "neuron.sync_frequency",
            u64,
            &settings.neuron.sync_frequency
        ),
    };

    let dht_config = DhtConfig {
        port: *get_config_value!(args, "dht.port", u16, &settings.dht.port),
        bootstrap_nodes: get_config_value!(
            args,
            "dht.bootstrap_nodes",
            Option<Vec<Multiaddr>>,
            &settings.dht.bootstrap_nodes
        )
        .clone(),
    };

    Ok(BaseNeuronConfig {
        version: Version::from_str(&settings.version)?,
        netuid: *get_config_value!(args, "netuid", u16, settings.netuid),
        wallet_path: expanduser(get_config_value!(
            args,
            "wallet_path",
            String,
            &settings.wallet_path
        ))?,
        wallet_name: get_config_value!(args, "wallet_name", String, &settings.wallet_name)
            .to_string(),
        hotkey_name: get_config_value!(args, "hotkey_name", String, &settings.hotkey_name)
            .to_string(),
        external_ip: get_config_value!(args, "external_ip", String, &settings.external_ip).clone(),
        api_port,
        quic_port: Some(quic_port), // TODO: surely there's a better way to do this
        post_ip: *get_config_value!(args, "post_ip", bool, &settings.post_ip),
        mock: *get_config_value!(args, "mock", bool, &settings.mock),
        load_old_nodes: *get_config_value!(args, "load_old_nodes", bool, &settings.load_old_nodes),
        min_stake_threshold: *get_config_value!(
            args,
            "min_stake_threshold",
            u64,
            &settings.min_stake_threshold
        ),
        db_file: expanduser(get_config_value!(
            args,
            "db_file",
            String,
            &settings.db_file
        ))?,
        dht_dir: expanduser(get_config_value!(
            args,
            "dht_dir",
            String,
            &settings.dht_dir
        ))?,
        pem_file: expanduser(get_config_value!(
            args,
            "pem_file",
            String,
            &settings.pem_file
        ))?,
        subtensor: subtensor_config,
        neuron: neuron_config,
        dht: dht_config,
    })
}
