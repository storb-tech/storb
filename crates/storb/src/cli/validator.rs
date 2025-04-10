use anyhow::Result;
use base::BaseNeuronConfig;
use clap::{Arg, ArgAction, ArgMatches, Command};
use expanduser::expanduser;
use storb_validator;
use storb_validator::validator::ValidatorConfig;

use super::args::get_neuron_config;
use crate::config::Settings;
use crate::get_config_value;

pub fn cli() -> Command {
    Command::new("validator")
        .about("Run a Storb validator")
        .args([
            // Validator settings
            Arg::new("scores_state_file")
                .long("scores-state-file")
                .value_name("path")
                .help("The path to the scores state file")
                .action(ArgAction::Set),
            Arg::new("api_keys_db")
                .long("api-keys-db")
                .value_name("path")
                .help("The path to the API keys database")
                .action(ArgAction::Set),
            // Neuron settings
            Arg::new("neuron.num_concurrent_forwards")
                .long("neuron.num-concurrent-forwards")
                .value_name("value")
                .help("The number of concurrent forwards running at any time")
                .action(ArgAction::Set),
            Arg::new("neuron.disable_set_weights")
                .long("neuron.disable-set-weights")
                .help("Disable weight setting")
                .action(ArgAction::SetTrue),
            Arg::new("neuron.moving_average_alpha")
                .long("neuron.moving-average-alpha")
                .value_name("alpha")
                .help("Moving average alpha parameter, how much to add of the new observation")
                .action(ArgAction::Set),
            Arg::new("neuron.response_time_alpha")
                .long("neuron.response-time-alpha")
                .value_name("alpha")
                .help("Moving average alpha parameter for response time scores")
                .action(ArgAction::Set),
            // Query settings
            Arg::new("query.batch_size")
                .long("query.batch-size")
                .value_name("size")
                .help("Max store query batch size")
                .action(ArgAction::Set),
            Arg::new("query.num_uids")
                .long("query.num-uids")
                .value_name("num_uids")
                .help("Max number of uids to query per store request")
                .action(ArgAction::Set),
            Arg::new("query.timeout")
                .long("query.timeout")
                .value_name("timeout")
                .help("Query timeout in seconds")
                .action(ArgAction::Set),
        ])
}

pub fn exec(args: &ArgMatches, settings: &Settings) -> Result<()> {
    let scores_state_file = expanduser(get_config_value!(
        args,
        "scores_state_file",
        String,
        settings.validator.scores_state_file
    ))?;
    let api_keys_db = expanduser(get_config_value!(
        args,
        "api_keys_db",
        String,
        settings.validator.api_keys_db
    ))?;

    // Get validator config with CLI overrides
    let neuron_config: BaseNeuronConfig = get_neuron_config(args, settings)?;
    let validator_config = ValidatorConfig {
        scores_state_file,
        moving_average_alpha: *get_config_value!(
            args,
            "neuron.moving_average_alpha",
            f64,
            &settings.validator.neuron.moving_average_alpha
        ),
        neuron_config,
        api_keys_db,
    };

    storb_validator::run(validator_config);
    Ok(())
}
