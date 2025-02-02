use clap::{Arg, ArgAction, ArgMatches, Command};

use base::BaseNeuronConfig;
use storb_miner;
use storb_miner::miner::MinerConfig;

use crate::config::Settings;

use super::args::get_neuron_config;

pub fn cli() -> Command {
    Command::new("miner")
        .about("Run a Storb miner")
        .args([Arg::new("store_dir")
            .long("store-dir")
            .value_name("directory")
            .help("Directory for the object store")
            .action(ArgAction::Set)])
}

pub fn exec(args: &ArgMatches, settings: &Settings) {
    // TODO
    _ = args;
    _ = settings;

    // Get validator config with CLI overrides
    let neuron_config: BaseNeuronConfig = get_neuron_config(args, settings);
    let miner_config = MinerConfig { neuron_config };

    storb_miner::run(miner_config);
}
