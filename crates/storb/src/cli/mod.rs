use clap::Command;

pub mod args;
pub mod miner;
pub mod validator;

pub fn builtin() -> Vec<Command> {
    vec![miner::cli(), validator::cli()]
}
