use std::{collections::HashSet, sync::Arc};

use dashmap::DashMap;
use tokio::sync::mpsc::Sender;
use tracing::{debug, trace, warn};

use super::dht::DhtCommand;
use crate::{
    constants::{PEER_VERIFICATION_FREQUENCY, PEER_VERIFICATION_TIMEOUT},
    AddressBook,
};

pub struct PeerVerifier {
    // Fields and methods for the PeerVerifier
    address_book: AddressBook,
    verified_peers: HashSet<libp2p::PeerId>,
    pending_verifications: Arc<DashMap<libp2p::PeerId, libp2p::identify::Info>>,
    command_sender: Sender<DhtCommand>,
}

impl PeerVerifier {
    // Constructor and other methods for the PeerVerifier
    pub fn new(
        address_book: AddressBook,
        pending_verifications: Arc<DashMap<libp2p::PeerId, libp2p::identify::Info>>,
        command_sender: Sender<DhtCommand>,
    ) -> Self {
        PeerVerifier {
            address_book,
            verified_peers: HashSet::new(),
            pending_verifications,
            command_sender,
        }
    }

    async fn verify_peer(&mut self, peer_id: libp2p::PeerId) -> bool {
        // Verify the peer and update the address book
        if self.verified_peers.contains(&peer_id) {
            return true;
        }

        let neurons_list = self.address_book.clone();
        if neurons_list.is_empty() {
            warn!("No neurons registered in the address book. Try again later.");
            return false;
        }
        if neurons_list.contains_key(&peer_id) {
            // Peer is registered
            trace!(
                "Peer {} verified: Hotkey found in registered neurons.",
                peer_id
            );
            self.verified_peers.insert(peer_id);
            self.pending_verifications.remove(&peer_id);
            return true;
        }

        false
    }

    pub fn run(mut self) {
        // Starts to verify peers in a separate thread
        tokio::spawn(async move {
            loop {
                debug!("Verifying peers...");

                let verify_iteration = async {
                    // Check for pending verifications: clone the collection to avoid borrowing issues
                    let pending: Vec<(libp2p::PeerId, libp2p::identify::Info)> = self
                        .pending_verifications
                        .iter()
                        .map(|r| (*r.key(), r.value().clone()))
                        .collect();
                    for (peer_id, info) in pending {
                        if self.verify_peer(peer_id).await {
                            debug!("Peer {} verified successfully.", peer_id);
                            let _ = self
                                .command_sender
                                .send(DhtCommand::ProcessVerificationResult {
                                    peer_id,
                                    info,
                                    result: Ok(true),
                                })
                                .await;
                        } else {
                            warn!("Failed to verify peer {}.", peer_id);
                        }
                    }
                };

                tokio::select! {
                    _ = verify_iteration => {},
                    _ = tokio::time::sleep(std::time::Duration::from_secs(PEER_VERIFICATION_TIMEOUT)) => {
                        warn!("Peer verification timeout reached");
                    }
                }

                // Sleep for a bit before the next verification cycle
                tokio::time::sleep(std::time::Duration::from_secs(PEER_VERIFICATION_FREQUENCY))
                    .await;

                debug!("Peer verification cycle completed.");
            }
        });
    }
}
