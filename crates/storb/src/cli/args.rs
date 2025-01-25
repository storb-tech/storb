//! Contains arguments that are used by multiple subcommands, e.g.
//! `miner` and `validator`.

use clap::{value_parser, Arg, ArgAction};

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
        Arg::new("ip_port")
            .long("ip-port")
            .value_name("port")
            .help("API port for the node")
            .action(ArgAction::Set)
            .global(true),
        Arg::new("post_ip")
            .long("post-ip")
            .help("Whether to post the IP to the chain or not")
            .action(ArgAction::SetTrue)
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
            .help("Minimum stake threshold")
            .action(ArgAction::Set)
            .global(true),
        Arg::new("log_level")
            .long("log-level")
            .value_name("level")
            .help("Set the log level")
            .action(ArgAction::Set)
            .global(true),
        Arg::new("db_dir")
            .long("db_dir")
            .value_name("path")
            .help("Path to the database")
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
        // Neuron
        Arg::new("neuron.sync_frequency")
            .long("neuron.sync-frequency")
            .value_name("frequency")
            .help("The default sync frequency for nodes")
            .action(ArgAction::Set)
            .global(true),
        // DHT
        Arg::new("dht.port")
            .long("dht.port")
            .value_name("port")
            .help("DHT port")
            .action(ArgAction::Set)
            .global(true),
        Arg::new("dht.file")
            .long("dht.file")
            .value_name("path")
            .help("Path to file for the DHT to save state")
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
            .help("Bootstrap node port for the DHT")
            .action(ArgAction::Set)
            .global(true),
    ]
}
