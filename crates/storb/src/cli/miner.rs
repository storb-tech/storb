use anyhow::Result;
use base::BaseNeuronConfig;
use clap::{Arg, ArgAction, ArgMatches, Command};
use expanduser::expanduser;
use storb_miner;
use storb_miner::miner::MinerConfig;

use super::args::get_neuron_config;
use crate::config::Settings;
use crate::get_config_value;

pub fn cli() -> Command {
    Command::new("miner")
        .about("Run a Storb miner")
        .args([Arg::new("store_dir")
            .long("store-dir")
            .value_name("directory")
            .help("Directory for the object store")
            .action(ArgAction::Set)])
}

pub fn exec(args: &ArgMatches, settings: &Settings) -> Result<()> {
    let store_dir = expanduser(get_config_value!(
        args,
        "store_dir",
        String,
        settings.miner.store_dir
    ))?;

    // Get validator config with CLI overrides
    let neuron_config: BaseNeuronConfig = get_neuron_config(args, settings)?;
    let miner_config = MinerConfig {
        neuron_config,
        store_dir,
    };

    storb_miner::run(miner_config);
    Ok(())
}
