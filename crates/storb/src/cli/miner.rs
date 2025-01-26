use clap::{Arg, ArgAction, ArgMatches, Command};

use crate::config::Settings;

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
}
