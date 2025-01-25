use args::common_args;
use clap::Command;

use crate::config::Settings;

mod args;
mod miner;
mod validator;

const NAME: &str = "storb";
const BIN_NAME: &str = "storb";
const VERSION: &str = env!("CARGO_PKG_VERSION");
const ABOUT: &str = env!("CARGO_PKG_DESCRIPTION");

fn builtin() -> Vec<Command> {
    vec![miner::cli(), validator::cli()]
}

pub fn cli(settings: &Settings) {
    let about_text = format!("{} {}\n{}", NAME, VERSION, ABOUT);
    let usage_text = format!("{} <command> [options] [<path>]", BIN_NAME);
    let after_help_text = format!(
        "See '{} help <command>' for more information on a command",
        BIN_NAME
    );

    let storb = Command::new("storb")
        .name(NAME)
        .version(VERSION)
        .about(about_text)
        .bin_name(BIN_NAME)
        .arg_required_else_help(true)
        .override_usage(usage_text)
        .after_help(after_help_text)
        .subcommands(builtin())
        .args(common_args());

    let matches = storb.get_matches();

    match matches.subcommand() {
        Some(("miner", cmd)) => miner::exec(cmd, settings),
        Some(("validator", cmd)) => validator::exec(cmd, settings),
        _ => unreachable!(),
    }
}
