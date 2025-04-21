use std::{collections::HashSet, sync::Arc, time::Duration};

use dashmap::DashMap;
use libp2p::{identify, PeerId};
use tokio::sync::{mpsc::Sender, watch};
use tracing::{debug, error, info, trace, warn};

use super::dht::DhtCommand;
use crate::{constants::PEER_VERIFICATION_FREQUENCY, AddressBook};

pub struct PeerVerifier {
    address_book: AddressBook,
    verified_peers: HashSet<PeerId>,
    pending_verifications: Arc<DashMap<PeerId, identify::Info>>,
    command_sender: Sender<DhtCommand>,
    shutdown_rx: watch::Receiver<bool>,
}

impl PeerVerifier {
    pub fn new(
        address_book: AddressBook,
        pending_verifications: Arc<DashMap<PeerId, identify::Info>>,
        command_sender: Sender<DhtCommand>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        PeerVerifier {
            address_book,
            verified_peers: HashSet::new(),
            pending_verifications,
            command_sender,
            shutdown_rx,
        }
    }

    fn verify_peer(&mut self, peer_id: &PeerId) -> bool {
        if self.verified_peers.contains(peer_id) {
            trace!("Peer {} found in local verified cache.", peer_id);
            return true;
        }

        if self.address_book.is_empty() {
            trace!("Address book is empty, cannot verify peer {}.", peer_id);
            return false;
        }

        if self.address_book.contains_key(peer_id) {
            trace!("Peer {} verified: Found in AddressBook.", peer_id);
            self.verified_peers.insert(*peer_id);
            true
        } else {
            trace!("Peer {} not found in AddressBook.", peer_id);
            false
        }
    }

    pub fn run(mut self) {
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(PEER_VERIFICATION_FREQUENCY));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            info!("PeerVerifier task started");

            loop {
                tokio::select! {

                    Ok(_) = self.shutdown_rx.changed() => {
                        if *self.shutdown_rx.borrow() {
                            info!("PeerVerifier received shutdown signal.");
                            break;
                        }
                    }

                    _ = interval.tick() => {
                        trace!("PeerVerifier tick: processing pending peers.");

                        let peer_keys: Vec<PeerId> = self.pending_verifications.iter().map(|e| *e.key()).collect();

                        if peer_keys.is_empty() {
                            trace!("No peers pending verification.");
                            continue;
                        }
                        debug!("Verifying {} pending peers.", peer_keys.len());

                        for peer_id in peer_keys {
                            let info = match self.pending_verifications.get(&peer_id) {
                                 Some(entry) => entry.value().clone(),
                                 None => {
                                     trace!("Peer {} no longer pending, skipping.", peer_id);
                                     continue;
                                 }
                            };

                            let is_registered = self.verify_peer(&peer_id);
                            let result = Ok(is_registered);

                            debug!("Verification result for {}: {}", peer_id, is_registered);

                            let cmd = DhtCommand::ProcessVerificationResult {
                                peer_id,
                                info: Box::new(info),
                                result,
                            };

                            if let Err(e) = self.command_sender.send(cmd).await {
                                 warn!("Failed to send verification result for peer {}: {}", peer_id, e);
                                 if self.command_sender.is_closed() {
                                     error!("Command channel to StorbDHT closed. PeerVerifier stopping.");
                                     return;
                                 }
                                 continue;
                            }

                            if let Some((removed_peer, _)) = self.pending_verifications.remove(&peer_id) {
                                  trace!("Removed peer {} from pending map after sending result.", removed_peer);
                             } else {
                                  trace!("Peer {} was already removed from pending map when verifier tried.", peer_id);
                             }
                        }
                        debug!("Finished verification cycle for peers.");
                    }
                }
            }
            info!("PeerVerifier task stopped.");
        });
    }
}
