use tokio;

use neuron::NeuronConfig;

pub struct ValidatorConfig {
    pub neuron_config: NeuronConfig,
}

struct Validator {
    config: ValidatorConfig,
}

impl Validator {
    pub fn new(config: ValidatorConfig) -> Self {
        Validator { config }
    }
}

/// Run the validator
pub fn run() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            println!("Hello from the Storb validator");
        })
}
