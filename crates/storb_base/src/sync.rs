use std::net::Ipv4Addr;

use crabtensor::api::runtime_apis::neuron_info_runtime_api::NeuronInfoRuntimeApi;
use crabtensor::api::runtime_types::pallet_subtensor::rpc_info::neuron_info::NeuronInfoLite;
use crabtensor::AccountId;
use tracing::{debug, error, info};

use crate::constants::CLIENT_TIMEOUT;
use crate::{BaseNeuron, LocalNodeInfo, NodeInfo};

pub trait Synchronizable {
    fn sync_metagraph(
        &mut self,
    ) -> impl std::future::Future<Output = Result<(), Box<dyn std::error::Error>>>;
    fn get_remote_node_info(
        addr: reqwest::Url,
    ) -> impl std::future::Future<Output = Result<LocalNodeInfo, Box<dyn std::error::Error>>>;
}

// Implementation for BaseNeuron
impl Synchronizable for BaseNeuron {
    async fn get_remote_node_info(
        addr: reqwest::Url,
    ) -> Result<LocalNodeInfo, Box<dyn std::error::Error>> {
        debug!("Requesting node info from: {}", addr.to_string());
        let req_client = reqwest::Client::builder()
            .timeout(CLIENT_TIMEOUT)
            .build()
            .map_err(|e| {
                error!("Failed to build HTTP client: {:?}", e);
                e
            })?;

        // Send the GET request.
        let response = req_client.get(addr).send().await.map_err(|e| {
            error!("Failed to send request: {:?}", e);
            e
        })?;

        // Read the response bytes.
        let data = response.bytes().await.map_err(|e| {
            error!("Failed to read response bytes: {:?}", e);
            e
        })?;

        debug!("get_remote_node_info - raw data: {:?}", data);

        // Deserialize the response using bincode.
        let local_node_info: LocalNodeInfo =
            bincode::deserialize::<LocalNodeInfo>(&data).map_err(|e| {
                error!("Failed to deserialize LocalNodeInfo: {:?}", e);
                e
            })?;

        Ok(local_node_info)
    }

    /// Synchronise the local metagraph state with chain.
    async fn sync_metagraph(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Syncing metagraph at block");
        let current_block = self
            .subtensor
            .blocks()
            .at_latest()
            .await
            .map_err(|e| format!("Failed to get latest block: {:?}", e))?;

        let runtime_api = self.subtensor.runtime_api().at(current_block.reference());
        let mut local_node_info = self.local_node_info.clone();
        // TODO: error out if cant access chain when calling above functions

        // TODO: is there a nicer way to pass self for NeuronInfoRuntimeApi?
        let neurons_payload =
            NeuronInfoRuntimeApi::get_neurons_lite(&NeuronInfoRuntimeApi {}, self.config.netuid);

        // TODO: error out if cant access chain when calling above function
        let neurons: Vec<NeuronInfoLite<AccountId>> = runtime_api.call(neurons_payload).await?;

        if local_node_info.uid.is_none() {
            // Find our local node uid or crash the program
            let my_neuron = Self::find_neuron_info(&neurons, self.signer.account_id())
                .expect("Local node not found in neuron list");
            local_node_info.uid = Some(my_neuron.uid);
        }

        self.local_node_info = local_node_info;

        for (neuron_uid, _) in neurons.iter().enumerate() {
            let neuron_info: NeuronInfoLite<AccountId> = neurons[neuron_uid].clone();
            // NOTE: if we don't run the second check we will query ourselves and it will
            // create a deadlock.
            if neuron_uid != self.local_node_info.uid.ok_or("Node UID did not exist")? as usize {
                // TODO: if hotkeys have changed reset score and statistics for the uid!

                let neuron_ip = &neuron_info.axon_info.ip;
                let neuron_port = &neuron_info.axon_info.port;

                // Check if its a valid port
                if *neuron_port == 0 {
                    error!("Invalid port for neuron: {:?}", neuron_info.uid);
                    continue;
                };

                info!(
                    "Getting peer id from neuron: {:?} at ip: {:?} and port: {:?}",
                    neuron_info.uid, neuron_ip, neuron_port
                );
                let ip = Ipv4Addr::from((*neuron_ip & 0xffff_ffff) as u32);
                let url_raw = format!("http://{}:{}/info", ip, neuron_port);
                let url = reqwest::Url::parse(&url_raw).expect("Invalid URL");
                let remote_node_info = Self::get_remote_node_info(url)
                    .await
                    .map_err(|e| format!("Failed to get remote node info: {:?}", e))?;

                let node_info = NodeInfo {
                    neuron_info,
                    peer_id: remote_node_info.peer_id,
                    http_address: remote_node_info.http_address,
                    quic_address: remote_node_info.quic_address,
                    version: remote_node_info.version,
                };
                self.address_book.write().await.insert(
                    remote_node_info
                        .peer_id
                        .ok_or("Peer ID did not exist so insertion into address book failed")?,
                    node_info,
                );
            }
        }

        Ok(())
    }
}
