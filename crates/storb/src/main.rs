mod cli;
mod config;
mod log;

pub fn main() {
    // CLI values take precedence over settings.toml
    let settings = match config::Settings::new(None) {
        Ok(s) => s,
        Err(error) => panic!("Failed to parse settings file: {error:?}"),
    };

    // println!("Settings: {settings:?}");

    cli::cli(&settings);
}
