//! Contains arguments that are used by multiple subcommands, e.g.
//! `miner` and `validator`.

use std::str::FromStr;

use anyhow::Result;
use base::version::Version;
use base::{BaseNeuronConfig, NeuronConfig, SubtensorConfig};
use clap::{value_parser, Arg, ArgAction, ArgMatches};
use expanduser::expanduser;

use crate::{config::Settings, get_config_value};

pub fn common_args() -> Vec<Arg> {
    vec![
        Arg::new("config")
            .long("config-file")
            .value_name("file")
            .help("Use a custom config file")
            .action(ArgAction::Set)
            .global(true),
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
        Arg::new("otel_api_key")
            .long("otel-api-key")
            .value_name("key")
            .help("API key for OpenTelemetry")
            .action(ArgAction::Set)
            .global(true),
        Arg::new("otel_endpoint")
            .long("otel-endpoint")
            .value_name("endpoint")
            .help("Endpoint for OpenTelemetry")
            .action(ArgAction::Set)
            .global(true),
        Arg::new("otel_service_name")
            .long("otel-service-name")
            .value_name("name")
            .help("Service name for OpenTelemetry")
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
        // Databases
        Arg::new("db_file")
            .long("db-file")
            .value_name("path")
            .help("Path to the score database file")
            .action(ArgAction::Set)
            .global(true),
        Arg::new("metadatadb_file")
            .long("metadatadb-file")
            .value_name("path")
            .help("Path to the metadata database file")
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
        insecure: args.get_flag("subtensor.insecure") || settings.subtensor.insecure,
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
        metadatadb_file: expanduser(get_config_value!(
            args,
            "metadatadb_file",
            String,
            &settings.metadatadb_file
        ))?,
        neurons_dir: expanduser(get_config_value!(
            args,
            "neurons_dir",
            String,
            &settings.neurons_dir
        ))?,
        subtensor: subtensor_config,
        neuron: NeuronConfig {
            sync_frequency: *get_config_value!(
                args,
                "neuron.sync_frequency",
                u64,
                &settings.neuron.sync_frequency
            ),
        },
        otel_api_key: get_config_value!(args, "otel_api_key", String, &settings.otel_api_key)
            .to_string(),
        otel_endpoint: get_config_value!(args, "otel_endpoint", String, &settings.otel_endpoint)
            .to_string(),
        otel_service_name: get_config_value!(
            args,
            "otel_service_name",
            String,
            &settings.otel_service_name
        )
        .to_string(),
    })
}
