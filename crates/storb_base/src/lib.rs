pub struct Version {
    pub major: u16,
    pub minor: u16,
    pub patch: u16,
}

pub struct NeuronConfig {
    netuid: u16,

    wallet_name: String,
    hotkey_name: String,
}

pub struct Neuron {
    pub config: NeuronConfig,
    // pub spec_version: Version,
}

impl Neuron {
    pub fn new(config: NeuronConfig) -> Self {
        Neuron { config }
    }

    /// Check whether the neuron is registered in the subnet or not
    pub fn check_registration() {}

    /// Synchronise the local metagraph state with chain
    pub fn sync_metagraph() {}
}
