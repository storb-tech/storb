use std::sync::Arc;

use libp2p::Multiaddr;
use rand::{rngs::StdRng, RngCore, SeedableRng};
use subxt::ext::codec::Compact;

use crate::validator::Validator;

pub fn generate_synthetic_data(size: usize) -> Vec<u8> {
    let mut data = vec![0u8; size]; // Create a vector of "size" bytes initialized to zero
    let mut rng: StdRng = SeedableRng::from_entropy();
    rng.fill_bytes(&mut data);
    data
}

pub async fn get_id_quic_uids(
    validator: Arc<Validator>,
) -> (Compact<u16>, Vec<Multiaddr>, Vec<u16>) {
    let neuron_guard = validator.neuron.read().await;
    let address_book_arc = neuron_guard.address_book.clone();
    let validator_id = match neuron_guard.clone().local_node_info.uid {
        Some(id) => Compact(id),
        None => {
            return (Compact(0), Vec::new(), Vec::new());
        }
    };

    let address_book = address_book_arc.clone();

    // Filter addresses and get associated UIDs
    let mut quic_addresses_with_uids = Vec::new();
    for entry in address_book.iter() {
        let peer_id = entry.key();
        let node_info = entry.value();
        if let Some(quic_addr) = node_info.quic_address.clone() {
            let neuron_guard = validator.neuron.read().await;
            let peer_id_to_uid = neuron_guard.peer_node_uid.clone();
            if let Some(uid) = peer_id_to_uid.get_by_left(peer_id) {
                quic_addresses_with_uids.push((quic_addr, *uid));
            }
        }
    }
    let quic_addresses: Vec<Multiaddr> = quic_addresses_with_uids
        .iter()
        .map(|(addr, _)| addr.clone())
        .collect();
    let miner_uids: Vec<u16> = quic_addresses_with_uids
        .iter()
        .map(|(_, uid)| *uid)
        .collect();

    (validator_id, quic_addresses, miner_uids)
}
