use clap::{Arg, ArgAction, ArgMatches, Command};

use storb_validator;

use crate::config::Settings;

pub fn cli() -> Command {
    Command::new("validator")
        .about("Run a Storb validator")
        .args([
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

pub fn exec(args: &ArgMatches, settings: &Settings) {
    // TODO
    _ = args;
    _ = settings;

    storb_validator::run();
}
