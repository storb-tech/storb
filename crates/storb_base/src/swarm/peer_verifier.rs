use std::{collections::HashSet, sync::Arc};

use dashmap::DashMap;
use tokio::sync::mpsc::Sender;
use tracing::{info, warn};

use super::dht::DhtCommand;
use crate::AddressBook;

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
            address_book: address_book,
            verified_peers: HashSet::new(),
            pending_verifications: pending_verifications,
            command_sender: command_sender,
        }
    }

    async fn verify_peer(&mut self, peer_id: libp2p::PeerId, info: libp2p::identify::Info) -> bool {
        // Verify the peer and update the address book
        if self.verified_peers.contains(&peer_id) {
            return true;
        }

        let neurons_list = self.address_book.clone();
        if neurons_list.is_empty() {
            warn!("No neurons registered in the address book. Try again later.");
            info!(
                "current pending verifications: {:?}",
                self.pending_verifications
            );
            info!("current address book: {:?}", self.address_book);
            return false;
        }
        if neurons_list.contains_key(&peer_id) {
            // Peer is registered
            info!(
                "Peer {} verified: Hotkey found in registered neurons.",
                peer_id
            );
            self.verified_peers.insert(peer_id.clone());
            self.pending_verifications.remove(&peer_id);
            return true;
        }

        return false;
    }

    pub fn run(mut self) {
        // Starts to verify peers in a separate thread
        tokio::spawn(async move {
            loop {
                // Check for pending verifications: clone the collection to avoid borrowing issues
                let pending: Vec<(libp2p::PeerId, libp2p::identify::Info)> = self
                    .pending_verifications
                    .iter()
                    .map(|r| (r.key().clone(), r.value().clone()))
                    .collect();
                for (peer_id, info) in pending {
                    if self.verify_peer(peer_id.clone(), info.clone()).await {
                        info!("Peer {} verified successfully.", peer_id);
                        info!("current address book: {:?}", self.address_book);
                        info!("current verified peers: {:?}", self.verified_peers);
                        let _ = self
                            .command_sender
                            .send(DhtCommand::ProcessVerificationResult {
                                peer_id: peer_id,
                                info: info,
                                result: Ok(true),
                            })
                            .await;
                    } else {
                        warn!("Failed to verify peer {}.", peer_id);
                    }
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        });
    }
}
