use libp2p::{
    futures::StreamExt,
    identify,
    identity::Keypair,
    kad::{
        self, AddProviderOk, BootstrapOk, GetProvidersOk, GetRecordOk, PeerRecord, ProgressStep,
        PutRecordOk, QueryId, QueryResult, Quorum, Record, RecordKey,
    },
    mdns, ping,
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
    Multiaddr, PeerId, StreamProtocol, SwarmBuilder,
};
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    num::NonZeroUsize,
    path::PathBuf,
    time::Duration,
};
use tokio::sync::{oneshot, watch, Mutex};
use tracing::{debug, trace};

use super::{db, models, store::StorbStore};

const STORB_KAD_PROTOCOL_NAME: StreamProtocol = StreamProtocol::new("/storb/kad/1.0.0");
const DHT_QUERY_TIMEOUT: u64 = 10;

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "StorbEvent")]
pub struct StorbBehaviour {
    pub kademlia: kad::Behaviour<StorbStore>,
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

enum QueryChannel {
    #[allow(dead_code)]
    Bootstrap(oneshot::Sender<Result<(), Box<dyn Error + Send + Sync>>>),
    GetRecord(
        usize,
        Vec<PeerRecord>,
        oneshot::Sender<Result<Vec<PeerRecord>, Box<dyn Error + Send + Sync>>>,
    ),
    PutRecord(oneshot::Sender<Result<(), Box<dyn Error + Send + Sync>>>),
    GetProviders(
        HashSet<PeerId>,
        oneshot::Sender<Result<HashSet<PeerId>, Box<dyn Error + Send + Sync>>>,
    ),
    StartProviding(oneshot::Sender<Result<(), Box<dyn Error + Send + Sync>>>),
}

pub struct StorbDHT {
    pub swarm: Swarm<StorbBehaviour>,
    // Watch channel to signal that bootstrap is complete.
    bootstrap_done_sender: watch::Sender<bool>,
    bootstrap_done: watch::Receiver<bool>,
    queries: std::sync::Arc<Mutex<HashMap<QueryId, QueryChannel>>>,
}

impl StorbDHT {
    pub fn new(db_dir: PathBuf, port: u16, keys: Keypair) -> Result<Self, Box<dyn Error>> {
        assert!(port < 65535, "Invalid port number");

        // Generate a local keypair and derive our peer ID.
        let local_peer_id = PeerId::from(keys.public());

        // Create the RocksDB store and our custom StorbStore.
        // (Propagate errors instead of panicking.)
        let db = db::RocksDBStore::new(db::RocksDBConfig {
            path: db_dir,
            max_batch_delay: Duration::from_millis(10),
            max_batch_size: 100,
        })?;
        let store = StorbStore::new(
            std::sync::Arc::new(db),
            NonZeroUsize::new(20).unwrap(), // 20 is non-zero.
            NonZeroUsize::new(20).unwrap(),
        );

        // Create Kademlia with a minimal configuration.
        let kad_config = kad::Config::new(STORB_KAD_PROTOCOL_NAME);
        let kademlia = kad::Behaviour::with_config(local_peer_id, store, kad_config);

        // Use mDNS for local peer discovery.
        let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?;

        // Configure Identify to gather basic peer metadata.
        let identify_config = identify::Config::new("0.0.1".to_string(), keys.public());
        let identify = identify::Behaviour::new(identify_config);

        // Use Ping to check connectivity.
        let ping = ping::Behaviour::new(ping::Config::default());

        // Build the combined behaviour.
        let behaviour = StorbBehaviour {
            kademlia,
            mdns,
            identify,
            ping,
        };

        // Build the Swarm using QUIC transport and Tokio.
        let mut swarm = SwarmBuilder::with_existing_identity(keys.clone())
            .with_tokio()
            .with_quic() // Using QUIC for transport.
            .with_behaviour(|_| Ok(behaviour))?
            .with_swarm_config(|cfg| cfg)
            .build();

        // Set Kademlia to server mode.
        swarm
            .behaviour_mut()
            .kademlia
            .set_mode(Some(kad::Mode::Server));

        // Listen on a QUIC multiaddress. (0.0.0.0 means all interfaces; port is ephemeral.)

        let listen_addr: Multiaddr = format!("/ip4/0.0.0.0/udp/{port}/quic-v1").parse()?;
        swarm.listen_on(listen_addr)?;

        // Create the watch channel that indicates when bootstrap is done.
        let (bootstrap_done_sender, bootstrap_done) = watch::channel(false);

        Ok(Self {
            swarm,
            bootstrap_done_sender,
            bootstrap_done,
            queries: std::sync::Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Injects Kademlia events into our logic and signals bootstrap completion via the watch channel.
    fn inject_kad_event(
        &mut self,
        event: kad::Event,
        queries: &mut HashMap<QueryId, QueryChannel>,
    ) {
        trace!("Injecting Kademlia event: {:?}", event);
        if let kad::Event::OutboundQueryProgressed {
            id,
            result,
            step: ProgressStep { last, .. },
            ..
        } = event
        {
            let query_id: QueryId = id.into();

            match result {
                QueryResult::Bootstrap(Ok(BootstrapOk {
                    num_remaining,
                    peer,
                })) => {
                    debug!(
                        "Bootstrap succeeded with {:?} remaining and {:?}",
                        num_remaining, peer
                    );
                    if num_remaining == 0 {
                        // Signal that bootstrap is complete via the watch channel.
                        self.bootstrap_done_sender.send(true).unwrap_or_else(|e| {
                            debug!("Failed to signal bootstrap completion: {:?}", e)
                        });
                        if let Some(QueryChannel::Bootstrap(ch)) = queries.remove(&query_id) {
                            let _ = ch.send(Ok(()));
                        }
                    }
                }
                QueryResult::Bootstrap(Err(e)) => {
                    debug!("Bootstrap failed: {:?}", e);
                    if let Some(QueryChannel::Bootstrap(ch)) = queries.remove(&query_id) {
                        let _ = ch.send(Err(e.into()));
                    }
                }
                QueryResult::GetRecord(Ok(res)) => {
                    if let Some(QueryChannel::GetRecord(ref mut quorum, ref mut records, _)) =
                        queries.get_mut(&query_id)
                    {
                        match res {
                            GetRecordOk::FoundRecord(record) => records.push(record),
                            GetRecordOk::FinishedWithNoAdditionalRecord { .. } => {}
                        }

                        if last || records.len() >= *quorum {
                            if let Some(mut q) = self.swarm.behaviour_mut().kademlia.query_mut(&id)
                            {
                                q.finish();
                            }
                            if let Some(QueryChannel::GetRecord(_, records, ch)) =
                                queries.remove(&query_id)
                            {
                                let _ = ch.send(Ok(records));
                            }
                        }
                    }
                }
                QueryResult::GetRecord(Err(e)) => {
                    trace!("Query failed: {:?}", e);
                    if let Some(QueryChannel::GetRecord(_, _, ch)) = queries.remove(&query_id) {
                        let _ = ch.send(Err(e.into()));
                    }
                }
                QueryResult::PutRecord(Ok(PutRecordOk { key })) => {
                    trace!("Put record succeeded: {:?}", key);
                    if let Some(QueryChannel::PutRecord(ch)) = queries.remove(&query_id) {
                        let _ = ch.send(Ok(()));
                    }
                }
                QueryResult::PutRecord(Err(e)) => {
                    trace!("Put record failed: {:?}", e);
                    if let Some(QueryChannel::PutRecord(ch)) = queries.remove(&query_id) {
                        let _ = ch.send(Err(e.into()));
                    }
                }
                QueryResult::GetProviders(Ok(res)) => {
                    let mut finish = false;
                    if let Some(QueryChannel::GetProviders(ref mut peers, _)) =
                        queries.get_mut(&query_id)
                    {
                        match res {
                            GetProvidersOk::FoundProviders { key, providers } => {
                                trace!("Found providers for {:?}: {:?}", key, providers);
                                peers.extend(providers);
                            }
                            GetProvidersOk::FinishedWithNoAdditionalRecord { .. } => {}
                        }

                        if last {
                            finish = true;
                        }
                    }
                    if finish {
                        if let Some(QueryChannel::GetProviders(peers, ch)) =
                            queries.remove(&query_id)
                        {
                            let _ = ch.send(Ok(peers));
                        }
                    }
                }
                QueryResult::GetProviders(Err(e)) => {
                    trace!("Query failed: {:?}", e);
                    if let Some(QueryChannel::GetProviders(_, ch)) = queries.remove(&query_id) {
                        let _ = ch.send(Err(e.into()));
                    }
                }
                QueryResult::StartProviding(Ok(AddProviderOk { key })) => {
                    trace!("Start providing succeeded for {:?}", key);
                    if let Some(QueryChannel::StartProviding(ch)) = queries.remove(&query_id) {
                        let _ = ch.send(Ok(()));
                    }
                }
                QueryResult::StartProviding(Err(e)) => {
                    trace!("Start providing failed: {:?}", e);
                    if let Some(QueryChannel::StartProviding(ch)) = queries.remove(&query_id) {
                        let _ = ch.send(Err(e.into()));
                    }
                }
                _ => {}
            }
        }
    }

    fn inject_mdns_event(&mut self, event: mdns::Event) {
        trace!("Injecting mDNS event: {:?}", event);

        match event {
            mdns::Event::Discovered(peers) => {
                for (peer_id, addr) in peers {
                    trace!("Discovered peer: {:?} at {:?}", peer_id, addr);
                    self.swarm
                        .behaviour_mut()
                        .kademlia
                        .add_address(&peer_id, addr);
                }
            }
            mdns::Event::Expired(peers) => {
                for (peer_id, addr) in peers {
                    trace!("Expired peer: {:?} at {:?}", peer_id, addr);
                    self.swarm
                        .behaviour_mut()
                        .kademlia
                        .remove_address(&peer_id, &addr);
                }
            }
        }
    }

    /// Run the swarm event loop.
    /// Queries awaiting bootstrap will now wait until the watch channel indicates completion.
    pub async fn run(mut self) -> Result<(), Box<dyn Error>> {
        loop {
            let event: SwarmEvent<StorbEvent> = self.swarm.select_next_some().await;
            let queries_ref = self.queries.clone();
            {
                let mut queries = queries_ref.lock().await;

                match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        debug!("Swarm is listening on {:?}", address);
                    }
                    SwarmEvent::Behaviour(event) => match event {
                        StorbEvent::Mdns(event) => {
                            self.inject_mdns_event(event);
                        }
                        StorbEvent::Kademlia(event) => {
                            self.inject_kad_event(event, &mut queries);
                        }
                        StorbEvent::Identify(event) => {
                            trace!("Identify event: {:?}", event);
                        }
                        StorbEvent::Ping(event) => {
                            trace!("Ping event: {:?}", event);
                        }
                    },
                    SwarmEvent::ConnectionEstablished {
                        peer_id,
                        connection_id,
                        endpoint,
                        num_established,
                        concurrent_dial_errors,
                        established_in,
                    } => {
                        trace!(
                            "Connection established: {:?} to {:?}",
                            connection_id,
                            peer_id
                        );
                        trace!("Endpoint: {:?}", endpoint);
                        trace!("Num established: {:?}", num_established);
                        trace!("Concurrent dial errors: {:?}", concurrent_dial_errors);
                        trace!("Established in: {:?}", established_in);
                    }
                    SwarmEvent::ConnectionClosed {
                        peer_id,
                        connection_id,
                        endpoint,
                        num_established,
                        cause,
                    } => {
                        trace!("Connection closed: {:?} to {:?}", connection_id, peer_id);
                        trace!("Endpoint: {:?}", endpoint);
                        trace!("Num established: {:?}", num_established);
                        trace!("Cause: {:?}", cause);
                    }
                    SwarmEvent::IncomingConnection {
                        connection_id,
                        local_addr,
                        send_back_addr,
                    } => {
                        trace!(
                            "Incoming connection: {:?} from {:?}",
                            connection_id,
                            local_addr
                        );
                        trace!("Send back address: {:?}", send_back_addr);
                    }
                    SwarmEvent::IncomingConnectionError {
                        connection_id,
                        local_addr,
                        send_back_addr,
                        error,
                    } => {
                        trace!(
                            "Incoming connection error: {:?} from {:?}",
                            connection_id,
                            local_addr
                        );
                        trace!("Send back address: {:?}", send_back_addr);
                        trace!("Error: {:?}", error);
                    }
                    SwarmEvent::OutgoingConnectionError {
                        connection_id,
                        peer_id,
                        error,
                    } => {
                        trace!(
                            "Outgoing connection error: {:?} to {:?}",
                            connection_id,
                            peer_id
                        );
                        trace!("Error: {:?}", error);
                    }
                    SwarmEvent::ExpiredListenAddr {
                        listener_id,
                        address,
                    } => {
                        trace!(
                            "Expired listen address: {:?} from {:?}",
                            listener_id,
                            address
                        );
                    }
                    SwarmEvent::ListenerClosed {
                        listener_id,
                        addresses,
                        reason,
                    } => {
                        trace!("Listener closed: {:?} from {:?}", listener_id, addresses);
                        trace!("Reason: {:?}", reason);
                    }
                    SwarmEvent::ListenerError { listener_id, error } => {
                        trace!("Listener error: {:?} from {:?}", listener_id, error);
                    }
                    SwarmEvent::Dialing {
                        peer_id,
                        connection_id,
                    } => {
                        trace!("Dialing: {:?} to {:?}", connection_id, peer_id);
                    }
                    SwarmEvent::NewExternalAddrCandidate { address } => {
                        trace!("New external address candidate: {:?}", address);
                    }
                    SwarmEvent::ExternalAddrConfirmed { address } => {
                        trace!("External address confirmed: {:?}", address);
                    }
                    SwarmEvent::ExternalAddrExpired { address } => {
                        trace!("External address expired: {:?}", address);
                    }
                    SwarmEvent::NewExternalAddrOfPeer { peer_id, address } => {
                        trace!(
                            "New external address of peer: {:?} at {:?}",
                            peer_id,
                            address
                        );
                    }
                    _ => {}
                }
            }
        }
    }

    pub async fn put_tracker_entry(
        &mut self,
        infohash: RecordKey,
        value: models::TrackerDHTValue,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.wait_for_bootstrap().await?;

        let serialized_value = models::serialize_dht_value(&models::DHTValue::Tracker(value))?;
        let record = Record {
            key: infohash.clone().into(),
            value: serialized_value,
            publisher: Some(self.swarm.local_peer_id().clone()),
            expires: None,
        };

        // Create a oneshot channel for the put operation.
        let (tx, rx) = oneshot::channel();
        // Issue the put and obtain a query ID.
        let id = self
            .swarm
            .behaviour_mut()
            .kademlia
            .put_record(record, Quorum::Majority)
            .map_err(|e| format!("Failed to store tracker entry: {:?}", e))?;
        {
            let mut queries = self.queries.lock().await;
            queries.insert(id.into(), QueryChannel::PutRecord(tx));
        }

        // Wait for the tx response with a timeout of 10 seconds.
        match tokio::time::timeout(Duration::from_secs(DHT_QUERY_TIMEOUT), rx).await {
            Ok(result) => match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(_)) => Err("Failed to store tracker entry".into()),
                Err(_) => Err("Timed out waiting for put_tracker_entry response.".into()),
            },
            Err(_) => Err("Timed out waiting for put_tracker_entry response.".into()),
        }
    }

    pub async fn get_tracker_entry(
        &mut self,
        infohash: RecordKey,
    ) -> Result<Option<models::TrackerDHTValue>, Box<dyn std::error::Error + Send + Sync>> {
        self.wait_for_bootstrap().await?;

        // Create a oneshot channel for the get operation.
        let (tx, rx) = oneshot::channel();
        let quorum = 1;
        let id = self
            .swarm
            .behaviour_mut()
            .kademlia
            .get_record(infohash.into());
        {
            let mut queries = self.queries.lock().await;
            queries.insert(id.into(), QueryChannel::GetRecord(quorum, vec![], tx));
        }
        let records = match tokio::time::timeout(Duration::from_secs(DHT_QUERY_TIMEOUT), rx).await {
            Ok(result) => match result {
                Ok(Ok(records)) => records,
                Ok(Err(_)) => return Err("Failed to get tracker entry".into()),
                Err(_) => return Err("Timed out waiting for get_tracker_entry response.".into()),
            },
            Err(_) => return Err("Timed out waiting for get_tracker_entry response.".into()),
        };
        if let Some(record) = records.first() {
            let dht_value = models::deserialize_dht_value(&record.record.value)?;
            if let models::DHTValue::Tracker(tracker) = dht_value {
                Ok(Some(tracker))
            } else {
                Err("Record retrieved is not a tracker entry".into())
            }
        } else {
            Ok(None)
        }
    }

    pub async fn put_chunk_entry(
        &mut self,
        chunk_key: RecordKey,
        value: models::ChunkDHTValue,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.wait_for_bootstrap().await?;

        let serialized_value = models::serialize_dht_value(&models::DHTValue::Chunk(value))?;
        let record = Record {
            key: chunk_key.clone().into(),
            value: serialized_value,
            publisher: Some(self.swarm.local_peer_id().clone()),
            expires: None,
        };

        let (tx, rx) = oneshot::channel();
        let id = self
            .swarm
            .behaviour_mut()
            .kademlia
            .put_record(record, Quorum::Majority)
            .map_err(|e| format!("Failed to store chunk entry: {:?}", e))?;
        {
            let mut queries = self.queries.lock().await;
            queries.insert(id.into(), QueryChannel::PutRecord(tx));
        }
        match tokio::time::timeout(Duration::from_secs(DHT_QUERY_TIMEOUT), rx).await {
            Ok(result) => match result {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return Err("Failed to store chunk entry".into()),
                Err(_) => return Err("Timed out waiting for put_chunk_entry response.".into()),
            },
            Err(_) => return Err("Timed out waiting for put_chunk_entry response.".into()),
        };
        Ok(())
    }

    pub async fn get_chunk_entry(
        &mut self,
        chunk_key: RecordKey,
    ) -> Result<Option<models::ChunkDHTValue>, Box<dyn std::error::Error + Send + Sync>> {
        self.wait_for_bootstrap().await?;

        let (tx, rx) = oneshot::channel();
        let quorum = 1;
        let id = self
            .swarm
            .behaviour_mut()
            .kademlia
            .get_record(chunk_key.into());
        {
            let mut queries = self.queries.lock().await;
            queries.insert(id.into(), QueryChannel::GetRecord(quorum, vec![], tx));
        }
        let records = match tokio::time::timeout(Duration::from_secs(DHT_QUERY_TIMEOUT), rx).await {
            Ok(result) => match result {
                Ok(Ok(records)) => records,
                Ok(Err(_)) => return Err("Failed to get chunk entry".into()),
                Err(_) => return Err("Timed out waiting for get_chunk_entry response.".into()),
            },
            Err(_) => return Err("Timed out waiting for get_chunk_entry response.".into()),
        };
        if let Some(record) = records.first() {
            let dht_value = models::deserialize_dht_value(&record.record.value)?;
            if let models::DHTValue::Chunk(chunk) = dht_value {
                Ok(Some(chunk))
            } else {
                Err("Record retrieved is not a chunk entry".into())
            }
        } else {
            Ok(None)
        }
    }

    pub async fn put_piece_entry(
        &mut self,
        piece_key: RecordKey,
        value: models::PieceDHTValue,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.wait_for_bootstrap().await?;

        let serialized_value = models::serialize_dht_value(&models::DHTValue::Piece(value))?;
        let record = Record {
            key: piece_key.clone().into(),
            value: serialized_value,
            publisher: Some(self.swarm.local_peer_id().clone()),
            expires: None,
        };

        let (tx, rx) = oneshot::channel();
        let id = self
            .swarm
            .behaviour_mut()
            .kademlia
            .put_record(record, Quorum::Majority)
            .map_err(|e| format!("Failed to store piece entry: {:?}", e))?;
        {
            let mut queries = self.queries.lock().await;
            queries.insert(id.into(), QueryChannel::PutRecord(tx));
        }
        match tokio::time::timeout(Duration::from_secs(DHT_QUERY_TIMEOUT), rx).await {
            Ok(result) => match result {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return Err("Failed to store piece entry".into()),
                Err(_) => return Err("Timed out waiting for put_piece_entry response.".into()),
            },
            Err(_) => return Err("Timed out waiting for put_piece_entry response.".into()),
        };
        Ok(())
    }

    pub async fn get_piece_entry(
        &mut self,
        piece_key: RecordKey,
    ) -> Result<Option<models::PieceDHTValue>, Box<dyn std::error::Error + Send + Sync>> {
        self.wait_for_bootstrap().await?;

        let (tx, rx) = oneshot::channel();
        let quorum = 1;
        let id = self
            .swarm
            .behaviour_mut()
            .kademlia
            .get_record(piece_key.into());
        {
            let mut queries = self.queries.lock().await;
            queries.insert(id.into(), QueryChannel::GetRecord(quorum, vec![], tx));
        }
        let records = match tokio::time::timeout(Duration::from_secs(DHT_QUERY_TIMEOUT), rx).await {
            Ok(result) => match result {
                Ok(Ok(records)) => records,
                Ok(Err(_)) => return Err("Failed to get piece entry".into()),
                Err(_) => return Err("Timed out waiting for get_piece_entry response.".into()),
            },
            Err(_) => return Err("Timed out waiting for get_piece_entry response.".into()),
        };
        if let Some(record) = records.first() {
            let dht_value = models::deserialize_dht_value(&record.record.value)?;
            if let models::DHTValue::Piece(piece) = dht_value {
                Ok(Some(piece))
            } else {
                Err("Record retrieved is not a piece entry".into())
            }
        } else {
            Ok(None)
        }
    }

    pub async fn start_providing_piece(
        &mut self,
        piece_key: RecordKey,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.wait_for_bootstrap().await?;

        let (tx, rx) = oneshot::channel();
        let id = self
            .swarm
            .behaviour_mut()
            .kademlia
            .start_providing(piece_key.into())
            .map_err(|e| format!("Failed to start providing piece: {:?}", e))?;
        {
            let mut queries = self.queries.lock().await;
            queries.insert(id.into(), QueryChannel::StartProviding(tx));
        }
        match tokio::time::timeout(Duration::from_secs(DHT_QUERY_TIMEOUT), rx).await {
            Ok(result) => match result {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return Err("Failed to start providing piece".into()),
                Err(_) => {
                    return Err("Timed out waiting for start_providing_piece response.".into())
                }
            },
            Err(_) => return Err("Timed out waiting for start_providing_piece response.".into()),
        };
        Ok(())
    }

    pub async fn get_piece_providers(
        &mut self,
        piece_key: RecordKey,
    ) -> Result<HashSet<PeerId>, Box<dyn std::error::Error + Send + Sync>> {
        self.wait_for_bootstrap().await?;

        let (tx, rx) = oneshot::channel();
        let id = self
            .swarm
            .behaviour_mut()
            .kademlia
            .get_providers(piece_key.into());
        {
            let mut queries = self.queries.lock().await;
            queries.insert(id.into(), QueryChannel::GetProviders(HashSet::new(), tx));
        }
        let providers = match tokio::time::timeout(Duration::from_secs(DHT_QUERY_TIMEOUT), rx).await
        {
            Ok(result) => match result {
                Ok(Ok(providers)) => providers,
                Ok(Err(_)) => return Err("Failed to get piece providers".into()),
                Err(_) => return Err("Timed out waiting for get_piece_providers response.".into()),
            },
            Err(_) => return Err("Timed out waiting for get_piece_providers response.".into()),
        };
        Ok(providers)
    }

    pub async fn remove_record(
        &mut self,
        key: RecordKey,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.wait_for_bootstrap().await?;
        self.swarm.behaviour_mut().kademlia.remove_record(&key);
        Ok(())
    }

    async fn wait_for_bootstrap(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        while !*self.bootstrap_done.borrow() {
            self.bootstrap_done.changed().await?;
        }
        Ok(())
    }
}

impl Drop for StorbDHT {
    fn drop(&mut self) {
        debug!("Dropping StorbDHT connections");
        let peers: Vec<PeerId> = self.swarm.connected_peers().copied().collect();
        for (idx, conn) in peers.iter().enumerate() {
            trace!("Dropping connection {:?} to {:?}", idx, conn);
            let _ = self.swarm.disconnect_peer_id(*conn);
        }

        debug!("Dropped all connections");
    }
}
