use anyhow::Result;
use clap::Command;

use constants::{ABOUT, BIN_NAME, NAME, VERSION};
use tracing::info;

mod cli;
mod config;
mod constants;
mod log;

pub fn main() -> Result<()> {
    let about_text = format!("{} {}\n{}", NAME, VERSION, ABOUT);
    let usage_text = format!("{} <command> [options] [<path>]", BIN_NAME);
    let after_help_text = format!(
        "See '{} help <command>' for more information on a command",
        BIN_NAME
    );

    let storb = Command::new("storb")
        .bin_name(BIN_NAME)
        .name(NAME)
        .version(VERSION)
        .about(about_text)
        .override_usage(usage_text)
        .after_help(after_help_text)
        .args(cli::args::common_args())
        .arg_required_else_help(true)
        .subcommands(cli::builtin())
        .subcommand_required(true);

    let matches = storb.get_matches();

    let config_file: Option<&str> = match matches.try_get_one::<String>("config") {
        Ok(config_path) => config_path.map(|s| s.as_str()),
        Err(error) => {
            fatal!("Error while parsing config file flag: {error}")
        }
    };

    // CLI values take precedence over settings.toml
    let settings = match config::Settings::new(config_file) {
        Ok(s) => s,
        Err(error) => fatal!("Failed to parse settings file: {error:?}"),
    };

    // Initialise logger and set the logging level
    let log_level_arg = match matches.try_get_one::<String>("log_level") {
        Ok(level) => level,
        Err(error) => {
            fatal!("Error while parsing log level flag: {error}");
        }
    };
    let log_level = match log_level_arg {
        Some(level) => level,
        None => &settings.log_level,
    };

    let _guards = log::new(log_level.as_str());
    info!("Initialised logger with log level {log_level}");

    match matches.subcommand() {
        Some(("miner", cmd)) => cli::miner::exec(cmd, &settings)?,
        Some(("validator", cmd)) => cli::validator::exec(cmd, &settings)?,
        Some(("apikey", cmd)) => cli::apikey_manager::handle_command(cmd)?,
        _ => unreachable!(),
    }

    Ok(())
}
