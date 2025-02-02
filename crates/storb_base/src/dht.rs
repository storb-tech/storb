use libp2p::{
    futures::StreamExt,
    identify, identity,
    kad::{self, store::MemoryStore},
    mdns, ping,
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
    Multiaddr, PeerId, StreamProtocol, SwarmBuilder,
};
use std::collections::HashSet;
use std::error::Error;
use std::sync::{Arc, Mutex};
use tracing::info;

/// The protocol name used by our custom DHT.
const STORB_KAD_PROTOCOL_NAME: StreamProtocol = StreamProtocol::new("/storb/kad/1.0.1");

/// A very simple peer manager struct that tracks banned and protected peers.
#[derive(Default, Debug)]
pub struct PeerManager {
    /// Peers we never disconnect or ban.
    protected_peers: Mutex<HashSet<PeerId>>,
    /// Peers we have chosen to ban and forcibly disconnect.
    banned_peers: Mutex<HashSet<PeerId>>,
}

impl PeerManager {
    pub fn new() -> Self {
        PeerManager::default()
    }

    /// Add a peer to the "protected" set.
    pub fn protect_peer(&self, peer_id: PeerId) {
        self.protected_peers.lock().unwrap().insert(peer_id);
    }

    /// Remove a peer from the "protected" set.
    pub fn unprotect_peer(&self, peer_id: PeerId) {
        self.protected_peers.lock().unwrap().remove(&peer_id);
    }

    /// Ban a peer. If that peer is not protected, we will disconnect it.
    pub fn ban_peer(&self, peer_id: PeerId) {
        // Only ban if it's not protected.
        let protected = self.protected_peers.lock().unwrap();
        if protected.contains(&peer_id) {
            println!("Cannot ban protected peer: {peer_id}");
            return;
        }
        drop(protected);

        self.banned_peers.lock().unwrap().insert(peer_id);
    }

    /// Check if the peer is banned.
    pub fn is_banned(&self, peer_id: &PeerId) -> bool {
        self.banned_peers.lock().unwrap().contains(peer_id)
    }
}

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "StorbEvent")]
pub struct StorbBehaviour {
    pub kademlia: kad::Behaviour<MemoryStore>,
    pub mdns: mdns::tokio::Behaviour,
    pub identify: identify::Behaviour,
    pub ping: ping::Behaviour,
}

#[derive(Debug)]
pub enum StorbEvent {
    Kademlia(kad::Event),
    Mdns(mdns::Event),
    Identify(identify::Event),
    Ping(ping::Event),
}

impl From<kad::Event> for StorbEvent {
    fn from(event: kad::Event) -> Self {
        StorbEvent::Kademlia(event)
    }
}

impl From<mdns::Event> for StorbEvent {
    fn from(event: mdns::Event) -> Self {
        StorbEvent::Mdns(event)
    }
}

impl From<identify::Event> for StorbEvent {
    fn from(event: identify::Event) -> Self {
        StorbEvent::Identify(event)
    }
}

impl From<ping::Event> for StorbEvent {
    fn from(event: ping::Event) -> Self {
        StorbEvent::Ping(event)
    }
}

pub struct StorbDHT {
    pub swarm: Swarm<StorbBehaviour>,
    pub peer_manager: Arc<PeerManager>,
}

impl StorbDHT {
    pub fn new() -> Result<Self, Box<dyn Error>> {
        // Generate a local keypair and derive our peer ID.
        let local_key = identity::Keypair::generate_ed25519();
        let local_peer_id = PeerId::from(local_key.public());

        let store = MemoryStore::new(local_peer_id);
        let mut kad_config = kad::Config::new(STORB_KAD_PROTOCOL_NAME);

        let kademlia = kad::Behaviour::with_config(local_peer_id, store, kad_config);
        let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?;
        let identify_config = identify::Config::new("0.0.1".to_string(), local_key.public());
        let identify = identify::Behaviour::new(identify_config);
        let ping = ping::Behaviour::new(ping::Config::default());

        let behaviour = StorbBehaviour {
            kademlia,
            mdns,
            identify,
            ping,
        };

        let peer_manager = Arc::new(PeerManager::new());

        let mut swarm = SwarmBuilder::with_existing_identity(local_key.clone())
            .with_tokio()
            .with_quic() // Quic transport
            .with_behaviour(|_| Ok(behaviour))? // Provide the combined behaviour
            .with_swarm_config(|cfg| cfg)
            .build();

        swarm
            .behaviour_mut()
            .kademlia
            .set_mode(Some(kad::Mode::Server));

        // Listen on a QUIC multiaddr (0.0.0.0 means listen on all interfaces, ephemeral port)
        // TODO: set the port to a fixed value.
        let listen_addr: Multiaddr = "/ip4/0.0.0.0/udp/0/quic-v1".parse()?;
        swarm.listen_on(listen_addr)?;

        Ok(Self {
            swarm,
            peer_manager,
        })
    }

    // Swarm event loop.
    pub async fn run(mut self) -> Result<(), Box<dyn Error>> {
        println!("Local peer ID: {:?}", self.swarm.local_peer_id());

        loop {
            match self.swarm.select_next_some().await {
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Swarm is listening on {:?}", address);
                }

                SwarmEvent::Behaviour(event) => match event {
                    StorbEvent::Mdns(mdns::Event::Discovered(list)) => {
                        for (peer_id, multiaddr) in list {
                            println!("mDNS discovered: {peer_id}, {multiaddr}");
                            self.swarm
                                .behaviour_mut()
                                .kademlia
                                .add_address(&peer_id, multiaddr);
                        }
                    }
                    StorbEvent::Mdns(mdns::Event::Expired(list)) => {
                        for (peer_id, _) in list {
                            println!("mDNS address expired for {peer_id}");
                        }
                    }

                    StorbEvent::Kademlia(kad_event) => {
                        println!("Kademlia event: {kad_event:?}");
                    }

                    StorbEvent::Identify(identify_event) => match identify_event {
                        identify::Event::Received { peer_id, info, .. } => {
                            info!(
                                "Identify: Peer {peer_id} => protocols: {:?}",
                                info.protocols
                            );
                        }
                        identify::Event::Sent {
                            connection_id,
                            peer_id,
                        } => {
                            info!("Identify sent to {peer_id}");
                        }
                        identify::Event::Pushed {
                            connection_id,
                            peer_id,
                            info,
                        } => {
                            info!(
                                "Identify pushed from {peer_id} => protocols: {:?}",
                                info.protocols
                            );
                        }
                        identify::Event::Error {
                            connection_id,
                            peer_id,
                            error,
                        } => {
                            info!("Identify error with {peer_id}: {error}");
                        }
                    },

                    StorbEvent::Ping(ping_event) => match ping_event.result {
                        Ok(rtt) => {
                            println!("Ping success with {:?}, rtt: {:?}", ping_event.peer, rtt);
                        }
                        Err(err) => {
                            println!("Ping failed with {:?}: {:?}", ping_event.peer, err);
                        }
                    },
                },

                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    // If the peer is banned, disconnect them immediately.
                    if self.peer_manager.is_banned(&peer_id) {
                        println!("Peer {peer_id} is banned. Disconnecting...");
                        let _ = self.swarm.disconnect_peer_id(peer_id);
                    } else {
                        println!("Connection established with {peer_id}");
                    }
                }

                SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
                    println!("Connection closed with {peer_id:?}, cause: {cause:?}");
                }

                // TODO: handle other SwarmEvent variants.
                _ => {}
            };
        }
    }
}
