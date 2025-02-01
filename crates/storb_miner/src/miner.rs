use neuron::NeuronConfig;

#[derive(Clone)]
pub struct MinerConfig {
    pub neuron_config: NeuronConfig,
}

/// The Storb miner
#[derive(Clone)]
pub struct Miner {
    config: MinerConfig,
}

pub impl Miner {
    pub fn new(config: MinerConfig) -> Self {
        Miner { config }
    }
}
