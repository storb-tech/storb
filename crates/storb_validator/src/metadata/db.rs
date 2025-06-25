use std::path::Path;
use std::sync::Arc;

use base::{
    piece::{ChunkHash, InfoHash, PieceHash},
    NodeUID,
};
use chrono::{Duration, Utc};
use crabtensor::{sign::KeypairSignature, AccountId};
use r2d2::Pool;
use r2d2_sqlite::{rusqlite::params, SqliteConnectionManager};
use rand::RngCore;
use rusqlite::{Connection, OptionalExtension, Result};
use subxt::ext::codec::Compact;
use tokio::sync::mpsc;
use tracing::{debug, error};

use super::models::{
    ChunkChallengeHistory, ChunkValue, InfohashValue, PieceChallengeHistory, SqlDateTime,
};
use crate::{
    constants::DB_MPSC_BUFFER_SIZE,
    metadata::models::{CrSqliteChanges, CrSqliteValue, PieceValue, SqlAccountId},
};

// Add type alias for the complex return type
pub type PiecesForRepairResult = Result<Vec<(PieceHash, Vec<NodeUID>)>, MetadataDBError>;

pub enum MetadataDBError {
    Database(rusqlite::Error),
    Pool(r2d2::Error),
    InvalidPath(String),
    MissingTable(String),
    ExtensionLoad(String),
    InvalidSignature,
    UnauthorizedAccess,
    InfohashNotFound,
    InvalidNonce,
    NonceExpired,
}

// add impl for converting MetadataDBError into a Box<dyn std::error::Error>
impl std::error::Error for MetadataDBError {}
impl std::fmt::Debug for MetadataDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetadataDBError::Database(e) => write!(f, "Database error: {:?}", e),
            MetadataDBError::Pool(e) => write!(f, "Pool error: {:?}", e),
            MetadataDBError::InvalidPath(path) => write!(f, "Invalid path: {}", path),
            MetadataDBError::MissingTable(table) => write!(f, "Missing table: {}", table),
            MetadataDBError::ExtensionLoad(msg) => write!(f, "Extension load error: {}", msg),
            MetadataDBError::InvalidSignature => write!(f, "Invalid signature"),
            MetadataDBError::UnauthorizedAccess => write!(f, "Unauthorized access"),
            MetadataDBError::InfohashNotFound => write!(f, "Infohash not found"),
            MetadataDBError::InvalidNonce => write!(f, "Invalid or expired nonce"),
            MetadataDBError::NonceExpired => write!(f, "Nonce has expired"),
        }
    }
}

impl std::fmt::Display for MetadataDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetadataDBError::Database(e) => write!(f, "Database error: {}", e),
            MetadataDBError::Pool(e) => write!(f, "Pool error: {}", e),
            MetadataDBError::InvalidPath(path) => write!(f, "Invalid path: {}", path),
            MetadataDBError::MissingTable(table) => write!(f, "Missing table: {}", table),
            MetadataDBError::ExtensionLoad(msg) => write!(f, "Extension load error: {}", msg),
            MetadataDBError::InvalidSignature => write!(f, "Invalid signature"),
            MetadataDBError::UnauthorizedAccess => write!(f, "Unauthorized access"),
            MetadataDBError::InfohashNotFound => write!(f, "Infohash not found"),
            MetadataDBError::InvalidNonce => write!(f, "Invalid or expired nonce"),
            MetadataDBError::NonceExpired => write!(f, "Nonce has expired"),
        }
    }
}

impl From<rusqlite::Error> for MetadataDBError {
    fn from(err: rusqlite::Error) -> Self {
        MetadataDBError::Database(err)
    }
}

impl From<r2d2::Error> for MetadataDBError {
    fn from(err: r2d2::Error) -> Self {
        MetadataDBError::Pool(err)
    }
}

pub enum MetadataDBCommand {
    GetSiteId {
        response_sender: mpsc::Sender<Result<Vec<u8>, MetadataDBError>>,
    },
    GetDBVersion {
        response_sender: mpsc::Sender<Result<u64, MetadataDBError>>,
    },
    InsertCrSqliteChanges {
        changes: Vec<CrSqliteChanges>,
        response_sender: mpsc::Sender<Result<(), MetadataDBError>>,
    },
    GetCrSqliteChanges {
        min_db_version: u64,
        site_id_exclude: Vec<u8>,
        response_sender: mpsc::Sender<Result<Vec<CrSqliteChanges>, MetadataDBError>>,
    },
    InsertObject {
        infohash_value: InfohashValue,
        chunks_with_pieces: Vec<(ChunkValue, Vec<PieceValue>)>,
        response_sender: mpsc::Sender<Result<(), MetadataDBError>>,
    },
    GetRandomChunk {
        response_sender: mpsc::Sender<Result<ChunkValue, MetadataDBError>>,
    },
    QueuePiecesForRepair {
        miner_uid: NodeUID,
        response_sender: mpsc::Sender<Result<(), MetadataDBError>>,
    },
    GetPiecesForRepair {
        response_sender: mpsc::Sender<PiecesForRepairResult>,
    },
    InsertPieceRepairHistory {
        piece_repair_history: PieceChallengeHistory,
        response_sender: mpsc::Sender<Result<(), MetadataDBError>>,
    },
    InsertChunkChallengeHistory {
        chunk_challenge_history: ChunkChallengeHistory,
        response_sender: mpsc::Sender<Result<(), MetadataDBError>>,
    },
    GetPiece {
        piece_hash: PieceHash,
        response_sender: mpsc::Sender<Result<PieceValue, MetadataDBError>>,
    },
    GetPiecesByChunk {
        chunk_hash: ChunkHash,
        response_sender: mpsc::Sender<Result<Vec<PieceValue>, MetadataDBError>>,
    },
    GetChunksByInfohash {
        infohash: Vec<u8>,
        response_sender: mpsc::Sender<Result<Vec<ChunkValue>, MetadataDBError>>,
    },
    GetInfohash {
        infohash: InfoHash,
        response_sender: mpsc::Sender<Result<InfohashValue, MetadataDBError>>,
    },
    GetPieceRepairHistory {
        piece_repair_hash: [u8; 32],
        response_sender: mpsc::Sender<Result<PieceChallengeHistory, MetadataDBError>>,
    },
    GetChunkChallengeHistory {
        challenge_hash: [u8; 32],
        response_sender: mpsc::Sender<Result<ChunkChallengeHistory, MetadataDBError>>,
    },
    DeleteInfohash {
        infohash_value: InfohashValue,
        response_sender: mpsc::Sender<Result<(), MetadataDBError>>,
    },
    GetInfohashesByOwner {
        owner_account_id: AccountId,
        response_sender: mpsc::Sender<Result<Vec<InfohashValue>, MetadataDBError>>,
    },
    VerifyOwnership {
        infohash: InfoHash,
        owner_account_id: AccountId,
        response_sender: mpsc::Sender<Result<bool, MetadataDBError>>,
    },
    GetNonce {
        account_id: AccountId,
        response_sender: mpsc::Sender<Result<[u8; 32], MetadataDBError>>,
    },
    GenerateNonce {
        account_id: AccountId,
        response_sender: mpsc::Sender<Result<[u8; 32], MetadataDBError>>,
    },
    ValidateAndConsumeNonce {
        account_id: AccountId,
        nonce: [u8; 32],
        response_sender: mpsc::Sender<Result<bool, MetadataDBError>>,
    },
    CleanupExpiredNonces {
        max_age: Duration,
        response_sender: mpsc::Sender<Result<u64, MetadataDBError>>, // Returns number of cleaned up nonces
    },
}

pub struct MetadataDB {
    pool: Arc<Pool<SqliteConnectionManager>>,
    command_receiver: mpsc::Receiver<MetadataDBCommand>,
}

impl MetadataDB {
    pub fn new(
        db_path: &Path,
        crsqlite_lib: &Path,
    ) -> Result<(Self, mpsc::Sender<MetadataDBCommand>), MetadataDBError> {
        // Validate paths exist
        if !db_path.exists() {
            return Err(MetadataDBError::InvalidPath(format!(
                "Database path does not exist: {}",
                db_path.display()
            )));
        }

        if !crsqlite_lib.exists() {
            return Err(MetadataDBError::InvalidPath(format!(
                "cr-sqlite extension library path does not exist: {}",
                crsqlite_lib.display()
            )));
        }

        // Create connection manager
        let crsqlite_lib = crsqlite_lib.to_path_buf();
        let manager = SqliteConnectionManager::file(db_path).with_init(move |conn| {
            Self::load_crsqlite_extension(conn, &crsqlite_lib).map_err(|e| match e {
                MetadataDBError::Database(e) => e,
                _ => rusqlite::Error::InvalidParameterName(e.to_string()),
            })?;
            Ok(())
        });

        // Create connection pool
        let pool = Pool::new(manager)?;

        // Test connection and validate database
        {
            let conn = pool.get()?;
            Self::validate_database(&conn)?;
        }

        // Upgrade all the tables crrs
        {
            let conn = pool.get()?;
            conn.execute_batch(
                "SELECT crsql_as_crr('infohashes');
                 SELECT crsql_as_crr('chunks');
                 SELECT crsql_as_crr('tracker_chunks');
                 SELECT crsql_as_crr('pieces');
                 SELECT crsql_as_crr('chunk_pieces');
                 SELECT crsql_as_crr('piece_repair_history');
                 SELECT crsql_as_crr('chunk_challenge_history');",
            )?;
        }

        let (command_sender, command_receiver) =
            mpsc::channel::<MetadataDBCommand>(DB_MPSC_BUFFER_SIZE);

        let db = Self {
            pool: Arc::new(pool),
            command_receiver,
        };

        Ok((db, command_sender))
    }

    fn load_crsqlite_extension(
        conn: &Connection,
        crsqlite_lib: &Path,
    ) -> Result<(), MetadataDBError> {
        unsafe {
            conn.load_extension_enable().map_err(|e| {
                MetadataDBError::ExtensionLoad(format!("Failed to enable extensions: {}", e))
            })?;

            let result = conn.load_extension(crsqlite_lib, Some("sqlite3_crsqlite_init"));

            // Always try to disable extensions, regardless of load result
            let disable_result = conn.load_extension_disable();

            // Handle the load result
            result.map_err(|e| {
                MetadataDBError::ExtensionLoad(format!("Failed to load extension: {}", e))
            })?;

            // Handle the disable result
            disable_result.map_err(|e| {
                MetadataDBError::ExtensionLoad(format!("Failed to disable extensions: {}", e))
            })?;

            Ok(())
        }
    }

    /// Checks if all required tables exist in the database
    fn validate_database(conn: &Connection) -> Result<(), MetadataDBError> {
        const REQUIRED_TABLES: &[&str] = &[
            "infohashes",
            "chunks",
            "tracker_chunks",
            "pieces",
            "chunk_pieces",
            "piece_repair_history",
            "chunk_challenge_history",
        ];

        for &table_name in REQUIRED_TABLES {
            let exists: bool = conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                params![table_name],
                |row| Ok(row.get::<_, i32>(0)? > 0),
            )?;

            if !exists {
                return Err(MetadataDBError::MissingTable(table_name.to_string()));
            }
        }
        Ok(())
    }

    pub async fn handle_get_site_id(
        &self,
        response_sender: mpsc::Sender<Result<Vec<u8>, MetadataDBError>>,
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);

        let site_id = tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let site_id: Vec<u8> =
                conn.query_row("SELECT site_id FROM crsql_site_id", [], |row| row.get(0))?;
            Ok::<Vec<u8>, MetadataDBError>(site_id)
        })
        .await
        .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        // Send the result through the response channel
        if response_sender.send(Ok(site_id)).await.is_err() {
            error!("Failed to send site ID response");
        }

        Ok(())
    }

    pub async fn get_site_id(
        command_sender: &mpsc::Sender<MetadataDBCommand>,
    ) -> Result<Vec<u8>, MetadataDBError> {
        // Create a channel for the response
        let (response_sender, mut response_receiver) =
            mpsc::channel::<Result<Vec<u8>, MetadataDBError>>(1);

        // Send the command to get site ID
        command_sender
            .send(MetadataDBCommand::GetSiteId { response_sender })
            .await
            .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))?;

        // Wait for the response
        match response_receiver.recv().await {
            Some(result) => result,
            None => Err(MetadataDBError::Database(
                rusqlite::Error::ExecuteReturnedResults,
            )),
        }
    }

    pub async fn handle_get_db_version(
        &self,
        response_sender: mpsc::Sender<Result<u64, MetadataDBError>>,
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);

        let db_version = tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let version: u64 = conn.query_row("SELECT crsql_db_version()", [], |row| row.get(0))?;
            Ok::<u64, MetadataDBError>(version)
        })
        .await
        .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        // Send the result through the response channel
        if response_sender.send(Ok(db_version)).await.is_err() {
            error!("Failed to send DB version response");
        }

        Ok(())
    }

    pub async fn get_db_version(
        command_sender: &mpsc::Sender<MetadataDBCommand>,
    ) -> Result<u64, MetadataDBError> {
        // Create a channel for the response
        let (response_sender, mut response_receiver) =
            mpsc::channel::<Result<u64, MetadataDBError>>(1);

        // Send the command to get DB version
        command_sender
            .send(MetadataDBCommand::GetDBVersion { response_sender })
            .await
            .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))?;

        // Wait for the response
        match response_receiver.recv().await {
            Some(result) => result,
            None => Err(MetadataDBError::Database(
                rusqlite::Error::ExecuteReturnedResults,
            )),
        }
    }

    pub async fn handle_insert_crsqlite_changes(
        &self,
        changes: Vec<CrSqliteChanges>,
        response_sender: mpsc::Sender<Result<(), MetadataDBError>>,
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);
        let changes = changes.clone();

        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let tx = conn.unchecked_transaction()?;

            for change in changes {
                tx.execute(
                    "INSERT INTO crsql_changes (`table`, pk, cid, val, col_version, db_version, site_id, cl, seq) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    params![
                        change.table,
                        change.pk,
                        change.cid,
                        change.val,
                        change.col_version,
                        change.db_version,
                        change.site_id,
                        change.cl,
                        change.seq
                    ],
                )?;
            }

            tx.commit()?;
            Ok::<(), MetadataDBError>(())
        }).await.map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        // Send the result through the response channel
        if response_sender.send(Ok(())).await.is_err() {
            error!("Failed to send insert changes response");
        }

        Ok(())
    }

    pub async fn insert_crsqlite_changes(
        command_sender: &mpsc::Sender<MetadataDBCommand>,
        changes: Vec<CrSqliteChanges>,
    ) -> Result<(), MetadataDBError> {
        // Create a channel for the response
        let (response_sender, mut response_receiver) =
            mpsc::channel::<Result<(), MetadataDBError>>(1);

        // Send the command to insert crsqlite changes
        command_sender
            .send(MetadataDBCommand::InsertCrSqliteChanges {
                changes,
                response_sender,
            })
            .await
            .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))?;

        // Wait for the response
        match response_receiver.recv().await {
            Some(result) => result,
            None => Err(MetadataDBError::Database(
                rusqlite::Error::ExecuteReturnedResults,
            )),
        }
    }

    pub async fn handle_get_crsqlite_changes(
        &self,
        min_db_version: u64,
        site_id_exclude: Vec<u8>,
        response_sender: mpsc::Sender<Result<Vec<CrSqliteChanges>, MetadataDBError>>,
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);
        let site_id_exclude = site_id_exclude.clone();

        let changes = tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let mut stmt = conn.prepare(
                "SELECT `table`, pk, cid, val, col_version, db_version, site_id, cl, seq
                 FROM crsql_changes
                 WHERE db_version > ?1 AND site_id NOT IN (?2)
                 ORDER BY db_version DESC",
            )?;

            let mut rows = stmt.query(params![min_db_version, site_id_exclude])?;
            let mut changes = Vec::new();
            while let Some(row) = rows.next()? {
                changes.push(CrSqliteChanges {
                    table: row.get(0)?,
                    pk: CrSqliteValue::from(row.get_ref(1)?),
                    cid: row.get(2)?,
                    val: CrSqliteValue::from(row.get_ref(3)?),
                    col_version: row.get(4)?,
                    db_version: row.get(5)?,
                    site_id: row.get(6)?,
                    cl: row.get(7)?,
                    seq: row.get(8)?,
                });
            }
            Ok::<Vec<CrSqliteChanges>, MetadataDBError>(changes)
        })
        .await
        .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        // Send the result through the response channel
        if response_sender.send(Ok(changes)).await.is_err() {
            error!("Failed to send crsqlite changes response");
        }

        Ok(())
    }

    pub async fn get_crsqlite_changes(
        command_sender: &mpsc::Sender<MetadataDBCommand>,
        min_db_version: u64,
        site_id_exclude: Vec<u8>,
    ) -> Result<Vec<CrSqliteChanges>, MetadataDBError> {
        // Create a channel for the response
        let (response_sender, mut response_receiver) =
            mpsc::channel::<Result<Vec<CrSqliteChanges>, MetadataDBError>>(1);

        // Send the command to get crsqlite changes
        command_sender
            .send(MetadataDBCommand::GetCrSqliteChanges {
                min_db_version,
                site_id_exclude,
                response_sender,
            })
            .await
            .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))?;

        // Wait for the response
        match response_receiver.recv().await {
            Some(result) => result,
            None => Err(MetadataDBError::Database(
                rusqlite::Error::ExecuteReturnedResults,
            )),
        }
    }

    /// Handles moving the pieces belonging to a miner from miner_pieces to pieces_to_repair
    pub async fn handle_queue_pieces_for_repair(
        &self,
        miner_uid: NodeUID,
        response_sender: mpsc::Sender<Result<(), MetadataDBError>>,
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);

        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let tx = conn.unchecked_transaction()?;

            // Collect all piece hashes first
            let pieces_to_repair = {
                let mut stmt =
                    tx.prepare("SELECT piece_hash FROM miner_pieces WHERE miner_uid = ?1")?;
                let mut rows = stmt.query(params![miner_uid])?;
                let mut pieces = Vec::new();

                while let Some(row) = rows.next()? {
                    let piece_hash: PieceHash = row.get(0)?;
                    pieces.push((piece_hash, miner_uid));
                }
                pieces
            }; // stmt and rows are dropped here automatically

            // Insert the pieces into pieces_to_repair
            // If the entry already exists, simply update the array of miners in the row
            for (piece_hash, miner) in pieces_to_repair {
                // Check if the piece already exists in pieces_to_repair
                let existing_miners: Option<String> = {
                    let mut check_stmt =
                        tx.prepare("SELECT miners FROM pieces_to_repair WHERE piece_hash = ?1")?;
                    check_stmt
                        .query_row(params![piece_hash], |row| row.get(0))
                        .optional()?
                }; // check_stmt is dropped here automatically

                if let Some(existing_miners) = existing_miners {
                    // Parse existing miners from JSON array
                    let mut miners: Vec<NodeUID> =
                        serde_json::from_str(&existing_miners).map_err(|e| {
                            MetadataDBError::Database(rusqlite::Error::FromSqlConversionFailure(
                                0,
                                rusqlite::types::Type::Text,
                                Box::new(e),
                            ))
                        })?;

                    // Add the new miner if not already present
                    if !miners.contains(&miner) {
                        miners.push(miner);
                    }

                    // Update the existing entry
                    tx.execute(
                        "UPDATE pieces_to_repair SET miners = ?1 WHERE piece_hash = ?2",
                        params![
                            serde_json::to_string(&miners).map_err(|e| {
                                MetadataDBError::Database(
                                    rusqlite::Error::FromSqlConversionFailure(
                                        0,
                                        rusqlite::types::Type::Text,
                                        Box::new(e),
                                    ),
                                )
                            })?,
                            piece_hash
                        ],
                    )?;
                } else {
                    // Insert a new entry
                    tx.execute(
                        "INSERT INTO pieces_to_repair (piece_hash, miners) VALUES (?1, ?2)",
                        params![
                            piece_hash,
                            serde_json::to_string(&vec![miner]).map_err(|e| {
                                MetadataDBError::Database(
                                    rusqlite::Error::FromSqlConversionFailure(
                                        0,
                                        rusqlite::types::Type::Text,
                                        Box::new(e),
                                    ),
                                )
                            })?
                        ],
                    )?;
                }
            }

            // Delete the pieces from miner_pieces
            tx.execute(
                "DELETE FROM miner_pieces WHERE miner_uid = ?1",
                params![miner_uid],
            )?;

            // Update the entries in the pieces table to remove the miner from its "miners" array
            tx.execute(
                "UPDATE pieces SET miners = json_remove(miners, uid.fullkey) FROM json_each(miners) as uid WHERE uid.value = ?1",
                params![miner_uid],
            )?;

            tx.commit()?;

            Ok::<(), MetadataDBError>(())
        })
        .await
        .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        // Send the result through the response channel
        if response_sender.send(Ok(())).await.is_err() {
            error!("Failed to send queue pieces for repair response");
        }

        Ok(())
    }

    /// Moves the pieces belonging to a miner from miner_pieces to pieces_to_repair
    pub async fn queue_pieces_for_repair(
        command_sender: &mpsc::Sender<MetadataDBCommand>,
        miner_uid: NodeUID,
    ) -> Result<(), MetadataDBError> {
        // Create a channel for the response
        let (response_sender, mut response_receiver) =
            mpsc::channel::<Result<(), MetadataDBError>>(1);

        // Send the command to queue pieces for repair
        command_sender
            .send(MetadataDBCommand::QueuePiecesForRepair {
                miner_uid,
                response_sender,
            })
            .await
            .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))?;

        // Wait for the response
        match response_receiver.recv().await {
            Some(result) => result,
            None => Err(MetadataDBError::Database(
                rusqlite::Error::ExecuteReturnedResults,
            )),
        }
    }

    // returns piece hashes that need repairing and the miners that lost the pieces
    pub async fn handle_get_pieces_for_repair(
        &self,
        response_sender: mpsc::Sender<PiecesForRepairResult>,
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);

        let pieces_for_repair = tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let mut stmt = conn.prepare("SELECT piece_hash, miners FROM pieces_to_repair")?;

            let mut rows = stmt.query([])?;
            let mut results = Vec::new();
            while let Some(row) = rows.next()? {
                let piece_hash: PieceHash = row.get(0)?;
                let miners: String = row.get(1)?;

                // Parse miners from JSON array
                let miners: Vec<NodeUID> = serde_json::from_str(&miners).map_err(|e| {
                    MetadataDBError::Database(rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    ))
                })?;

                results.push((piece_hash, miners));
            }
            Ok::<Vec<(PieceHash, Vec<NodeUID>)>, MetadataDBError>(results)
        })
        .await
        .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        // Send the result through the response channel
        if response_sender.send(Ok(pieces_for_repair)).await.is_err() {
            error!("Failed to send pieces for repair response");
        }

        Ok(())
    }

    pub async fn get_pieces_for_repair(
        command_sender: &mpsc::Sender<MetadataDBCommand>,
    ) -> Result<Vec<(PieceHash, Vec<NodeUID>)>, MetadataDBError> {
        // Create a channel for the response
        let (response_sender, mut response_receiver) = mpsc::channel::<PiecesForRepairResult>(1);

        // Send the command to get pieces for repair
        command_sender
            .send(MetadataDBCommand::GetPiecesForRepair { response_sender })
            .await
            .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))?;

        // Wait for the response
        match response_receiver.recv().await {
            Some(result) => result,
            None => Err(MetadataDBError::Database(
                rusqlite::Error::ExecuteReturnedResults,
            )),
        }
    }

    pub async fn handle_insert_piece_repair_history(
        &self,
        value: &PieceChallengeHistory,
        response_sender: mpsc::Sender<Result<(), MetadataDBError>>,
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);
        let value = value.clone();

        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let tx = conn.unchecked_transaction()?;

            tx.execute(
                "INSERT INTO piece_repair_history (piece_repair_hash, piece_hash, chunk_hash, validator_id, timestamp, signature) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    value.piece_repair_hash,
                    value.piece_hash,
                    value.chunk_hash,
                    value.validator_id.0,
                    SqlDateTime(value.timestamp),
                    value.signature.0.to_vec()
                ],
            )?;

            tx.commit()?;
            Ok::<(), MetadataDBError>(())
        }).await.map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        // Send the result through the response channel
        if response_sender.send(Ok(())).await.is_err() {
            error!("Failed to send piece repair history response");
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub async fn insert_piece_repair_history(
        command_sender: &mpsc::Sender<MetadataDBCommand>,
        piece_repair_history: PieceChallengeHistory,
    ) -> Result<(), MetadataDBError> {
        // Create a channel for the response
        let (response_sender, mut response_receiver) =
            mpsc::channel::<Result<(), MetadataDBError>>(1);

        // Send the command to insert piece repair history
        command_sender
            .send(MetadataDBCommand::InsertPieceRepairHistory {
                piece_repair_history,
                response_sender,
            })
            .await
            .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))?;

        // Wait for the response
        match response_receiver.recv().await {
            Some(result) => result,
            None => Err(MetadataDBError::Database(
                rusqlite::Error::ExecuteReturnedResults,
            )),
        }
    }

    pub async fn handle_insert_chunk_challenge_history(
        &self,
        value: &ChunkChallengeHistory,
        response_sender: mpsc::Sender<Result<(), MetadataDBError>>,
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);
        let value = value.clone();

        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let tx = conn.unchecked_transaction()?;

            // convert miners_challenged and miners_successful to json strings
            let miners_challenged = serde_json::to_string(&value.miners_challenged).map_err(|e| {
                MetadataDBError::Database(rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                ))
            })?;
            let miners_successful = serde_json::to_string(&value.miners_successful).map_err(|e| {
                MetadataDBError::Database(rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                ))
            })?;

            tx.execute(
                "INSERT INTO chunk_challenge_history (challenge_hash, chunk_hash, validator_id, miners_challenged, miners_successful, piece_repair_hash, timestamp, signature) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    value.challenge_hash,
                    value.chunk_hash,
                    value.validator_id.0,
                    miners_challenged,
                    miners_successful,
                    value.piece_repair_hash,
                    SqlDateTime(value.timestamp),
                    value.signature.0.to_vec()
                ],
            )?;

            tx.commit()?;
            Ok::<(), MetadataDBError>(())
        }).await.map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        // Send the result through the response channel
        if response_sender.send(Ok(())).await.is_err() {
            error!("Failed to send chunk challenge history response");
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub async fn insert_chunk_challenge_history(
        command_sender: &mpsc::Sender<MetadataDBCommand>,
        chunk_challenge_history: ChunkChallengeHistory,
    ) -> Result<(), MetadataDBError> {
        // Create a channel for the response
        let (response_sender, mut response_receiver) =
            mpsc::channel::<Result<(), MetadataDBError>>(1);

        // Send the command to insert chunk challenge history
        command_sender
            .send(MetadataDBCommand::InsertChunkChallengeHistory {
                chunk_challenge_history,
                response_sender,
            })
            .await
            .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))?;

        // Wait for the response
        match response_receiver.recv().await {
            Some(result) => result,
            None => Err(MetadataDBError::Database(
                rusqlite::Error::ExecuteReturnedResults,
            )),
        }
    }

    pub async fn handle_get_nonce(
        &self,
        account_id: &AccountId,
        response_sender: mpsc::Sender<Result<[u8; 32], MetadataDBError>>,
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);
        let account_id = account_id.clone();

        let nonce = tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let nonce: Vec<u8> = conn.query_row(
                "SELECT nonce FROM account_nonces WHERE account_id = ?1 ORDER BY timestamp DESC LIMIT 1",
                params![SqlAccountId(account_id)],
                |row| row.get(0),
            )?;

            if nonce.len() != 32 {
                return Err(MetadataDBError::InvalidNonce);
            }

            Ok::<[u8; 32], MetadataDBError>(nonce.try_into().unwrap())
        })
        .await
        .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        if response_sender.send(Ok(nonce)).await.is_err() {
            error!("Failed to send get nonce response");
        }
        Ok(())
    }

    pub async fn get_nonce(
        command_sender: &mpsc::Sender<MetadataDBCommand>,
        account_id: AccountId,
    ) -> Result<[u8; 32], MetadataDBError> {
        let (response_sender, mut response_receiver) =
            mpsc::channel::<Result<[u8; 32], MetadataDBError>>(1);

        command_sender
            .send(MetadataDBCommand::GetNonce {
                account_id,
                response_sender,
            })
            .await
            .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))?;

        match response_receiver.recv().await {
            Some(result) => result,
            None => Err(MetadataDBError::Database(
                rusqlite::Error::ExecuteReturnedResults,
            )),
        }
    }

    pub async fn handle_generate_nonce(
        &self,
        account_id: &AccountId,
        response_sender: mpsc::Sender<Result<[u8; 32], MetadataDBError>>,
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);
        let account_id = account_id.clone();

        let nonce = tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;

            // Generate a cryptographically secure random nonce
            let mut nonce = [0u8; 32];
            rand::thread_rng().fill_bytes(&mut nonce);

            // Store the nonce in the database
            let account_id_clone = account_id.clone();
            conn.execute(
                "INSERT INTO account_nonces (account_id, nonce, timestamp) VALUES (?1, ?2, ?3)",
                params![
                    SqlAccountId(account_id),
                    nonce.to_vec(),
                    SqlDateTime(Utc::now())
                ],
            )?;

            debug!(
                "Generated nonce for account {}: {}",
                account_id_clone,
                hex::encode(nonce)
            );

            Ok::<[u8; 32], MetadataDBError>(nonce)
        })
        .await
        .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        if response_sender.send(Ok(nonce)).await.is_err() {
            error!("Failed to send generate nonce response");
        }
        Ok(())
    }

    pub async fn generate_nonce(
        command_sender: &mpsc::Sender<MetadataDBCommand>,
        account_id: AccountId,
    ) -> Result<[u8; 32], MetadataDBError> {
        let (response_sender, mut response_receiver) =
            mpsc::channel::<Result<[u8; 32], MetadataDBError>>(1);

        command_sender
            .send(MetadataDBCommand::GenerateNonce {
                account_id,
                response_sender,
            })
            .await
            .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))?;

        match response_receiver.recv().await {
            Some(result) => result,
            None => Err(MetadataDBError::Database(
                rusqlite::Error::ExecuteReturnedResults,
            )),
        }
    }

    pub async fn handle_validate_and_consume_nonce(
        &self,
        account_id: &AccountId,
        nonce: &[u8; 32],
        response_sender: mpsc::Sender<Result<bool, MetadataDBError>>,
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);
        let account_id = account_id.clone();
        let nonce = *nonce;

        debug!(
            "Validating nonce for account {}: {}",
            account_id,
            hex::encode(nonce)
        );

        let is_valid = tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let tx = conn.unchecked_transaction()?;

            // Check if the nonce exists for this account and is not expired (within 1 hour)
            let nonce_exists: Result<bool, _> = tx.query_row(
                "SELECT 1 FROM account_nonces
                 WHERE account_id = ?1 AND nonce = ?2
                 ORDER BY timestamp DESC",
                params![SqlAccountId(account_id.clone()), nonce.to_vec()],
                |_| Ok(true),
            );

            match nonce_exists {
                Ok(_) => {
                    // Nonce is valid, consume it (delete it to prevent reuse)
                    debug!("Valid nonce found for account: {}, deleting it", account_id);
                    tx.execute(
                        "DELETE FROM account_nonces WHERE account_id = ?1 AND nonce = ?2",
                        params![SqlAccountId(account_id.clone()), nonce.to_vec()],
                    )?;
                    tx.commit()?;
                    Ok::<bool, MetadataDBError>(true)
                }
                Err(_) => {
                    // Nonce doesn't exist or is expired
                    Ok::<bool, MetadataDBError>(false)
                }
            }
        })
        .await
        .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        if response_sender.send(Ok(is_valid)).await.is_err() {
            error!("Failed to send validate nonce response");
        }
        debug!("Nonce validation result: {}", is_valid);
        Ok(())
    }

    pub async fn validate_and_consume_nonce(
        command_sender: &mpsc::Sender<MetadataDBCommand>,
        account_id: AccountId,
        nonce: &[u8; 32],
    ) -> Result<bool, MetadataDBError> {
        let (response_sender, mut response_receiver) =
            mpsc::channel::<Result<bool, MetadataDBError>>(1);

        command_sender
            .send(MetadataDBCommand::ValidateAndConsumeNonce {
                account_id,
                nonce: *nonce,
                response_sender,
            })
            .await
            .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))?;

        match response_receiver.recv().await {
            Some(result) => result,
            None => Err(MetadataDBError::Database(
                rusqlite::Error::ExecuteReturnedResults,
            )),
        }
    }

    pub async fn handle_cleanup_expired_nonces(
        &self,
        max_age: Duration,
        response_sender: mpsc::Sender<Result<u64, MetadataDBError>>,
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);

        let deleted_count = tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;

            // Compute cutoff time in Rust
            let cutoff_time = Utc::now() - max_age;

            let rows_affected = conn.execute(
                "DELETE FROM account_nonces WHERE timestamp < ?1",
                params![SqlDateTime(cutoff_time)],
            )?;

            Ok::<u64, MetadataDBError>(rows_affected as u64)
        })
        .await
        .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        if response_sender.send(Ok(deleted_count)).await.is_err() {
            error!("Failed to send cleanup nonces response");
        }
        Ok(())
    }

    pub async fn cleanup_expired_nonces(
        command_sender: &mpsc::Sender<MetadataDBCommand>,
        max_age: Duration,
    ) -> Result<u64, MetadataDBError> {
        let (response_sender, mut response_receiver) =
            mpsc::channel::<Result<u64, MetadataDBError>>(1);

        command_sender
            .send(MetadataDBCommand::CleanupExpiredNonces {
                max_age,
                response_sender,
            })
            .await
            .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))?;

        match response_receiver.recv().await {
            Some(result) => result,
            None => Err(MetadataDBError::Database(
                rusqlite::Error::ExecuteReturnedResults,
            )),
        }
    }

    // Inserts all the metadata for the object in bulk and sends result via response_sender
    pub async fn handle_insert_object(
        &self,
        infohash_value: &InfohashValue,
        chunks_with_pieces: Vec<(ChunkValue, Vec<PieceValue>)>,
        response_sender: mpsc::Sender<Result<(), MetadataDBError>>,
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);
        let infohash_value = infohash_value.clone();

        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let tx = conn.unchecked_transaction()?;

            // Insert infohash with owner and nonce
            match tx.execute(
                "INSERT INTO infohashes (infohash, name, length, chunk_size, chunk_count, owner_account_id, creation_timestamp, signature) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    infohash_value.infohash,
                    infohash_value.name,
                    infohash_value.length,
                    infohash_value.chunk_size,
                    infohash_value.chunk_count,
                    infohash_value.owner_account_id,
                    SqlDateTime(infohash_value.creation_timestamp),
                    infohash_value.signature.0.to_vec()
                ],
            ) {
                Ok(_) => {}
                Err(e) if e.to_string().contains("UNIQUE constraint failed") => {
                    debug!("Infohash {} already exists, skipping insert", hex::encode(infohash_value.infohash));
                }
                Err(e) => return Err(MetadataDBError::Database(e)),
            }

            // Prepare statements for better performance with multiple inserts
            let mut chunk_stmt = tx.prepare(
                "INSERT INTO chunks (chunk_hash, k, m, chunk_size, padlen, original_chunk_size, ref_count) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1)
                 ON CONFLICT(chunk_hash) DO UPDATE SET ref_count = ref_count + 1"
            )?;

            let mut tracker_chunk_stmt = tx.prepare(
                "INSERT INTO tracker_chunks (infohash, chunk_idx, chunk_hash) VALUES (?1, ?2, ?3)",
            )?;

            let piece_stmt = tx.prepare(
                "INSERT INTO pieces (piece_hash, piece_size, piece_type, miners, ref_count) VALUES (?1, ?2, ?3, ?4, 1)
                 ON CONFLICT(piece_hash) DO UPDATE SET
                   miners = ?4,
                   ref_count = ref_count + 1"
            )?;

            let mut chunk_pieces_stmt = tx.prepare(
                "INSERT INTO chunk_pieces (chunk_hash, piece_idx, piece_hash) VALUES (?1, ?2, ?3)",
            )?;

            // Insert chunks and their pieces
            // Iterate over each chunk and its associated pieces, also ensure that we can get the index
            for (chunk_idx, (chunk, pieces)) in chunks_with_pieces.iter().enumerate() {
                // Insert chunk
                // if it already exists, it will be ignored due to the unique constraint
                match chunk_stmt.execute(params![
                    chunk.chunk_hash,
                    chunk.k,
                    chunk.m,
                    chunk.chunk_size,
                    chunk.padlen,
                    chunk.original_chunk_size
                ]) {
                    Ok(_) => {}
                    Err(e) if e.to_string().contains("UNIQUE constraint failed") => {
                        // Ignore if chunk already exists
                        debug!("Chunk {} already exists, skipping insert", hex::encode(chunk.chunk_hash));
                    }
                    Err(e) => return Err(MetadataDBError::Database(e)),
                }


                // Insert chunk-infohash mapping
                // If it already exists, it will be ignored due to the unique constraint
                match tracker_chunk_stmt.execute(params![
                    infohash_value.infohash,
                    chunk_idx,
                    chunk.chunk_hash
                ]) {
                    Ok(_) => {}
                    Err(e) if e.to_string().contains("UNIQUE constraint failed") => {
                        // Ignore if chunk-infohash mapping already exists
                        debug!("Chunk {} for infohash {} already exists, skipping insert", hex::encode(chunk.chunk_hash), hex::encode(infohash_value.infohash));
                    }
                    Err(e) => return Err(MetadataDBError::Database(e)),
                };

                // Insert all pieces for this chunk
                for (piece_idx, piece) in pieces.iter().enumerate() {
                    // Convert miners to JSON string
                    let miners = serde_json::to_string(&piece.miners).map_err(|e| {
                        MetadataDBError::Database(rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        ))
                    })?;

                    // First try to insert the piece normally
                    let insert_result = tx.execute(
                        "INSERT INTO pieces (piece_hash, piece_size, piece_type, miners, ref_count) VALUES (?1, ?2, ?3, ?4, 1)",
                        params![
                            piece.piece_hash,
                            piece.piece_size,
                            piece.piece_type.clone() as u8,
                            miners
                        ]
                    );

                    match insert_result {
                        Ok(_) => {
                            // Piece inserted successfully
                            // Insert miners for this piece into miner_pieces
                            debug!("Inserting miners for piece {}", hex::encode(piece.piece_hash));
                            for miner in &piece.miners {
                                match tx.execute(
                                    "INSERT INTO miner_pieces (miner_uid, piece_hash) VALUES (?1, ?2)",
                                    params![miner.0, piece.piece_hash],
                                ) {
                                    Ok(_) => {
                                        debug!("Inserted miner {} for piece {}", miner.0, hex::encode(piece.piece_hash));
                                    }
                                    Err(e) if e.to_string().contains("UNIQUE constraint failed") => {
                                        // Ignore if miner-piece mapping already exists
                                        debug!("Miner {} for piece {} already exists, skipping insert", miner.0, hex::encode(piece.piece_hash));
                                    }
                                    Err(e) => return Err(MetadataDBError::Database(e)),
                                }
                            }
                        }
                        Err(e) if e.to_string().contains("UNIQUE constraint failed") => {
                            // Piece already exists, need to merge miners and update ref_count
                            let existing_miners: String = tx.query_row(
                                "SELECT miners FROM pieces WHERE piece_hash = ?1",
                                params![piece.piece_hash],
                                |row| row.get(0)
                            ).map_err(MetadataDBError::Database)?;

                            let mut existing_miners: Vec<Compact<NodeUID>> =
                                serde_json::from_str(&existing_miners).map_err(|e| {
                                    MetadataDBError::Database(rusqlite::Error::FromSqlConversionFailure(
                                        0,
                                        rusqlite::types::Type::Text,
                                        Box::new(e),
                                    ))
                                })?;

                            // Add new miners to the existing miners
                            for miner in &piece.miners {
                                if !existing_miners.contains(miner) {
                                    existing_miners.push(*miner);
                                }
                            }

                            // Update the piece with the new miners and increment ref_count
                            let updated_miners = serde_json::to_string(&existing_miners).map_err(
                                |e| {
                                    MetadataDBError::Database(rusqlite::Error::FromSqlConversionFailure(
                                        0,
                                        rusqlite::types::Type::Text,
                                        Box::new(e),
                                    ))
                                },
                            )?;

                            tx.execute(
                                "UPDATE pieces SET miners = ?1, ref_count = ref_count + 1 WHERE piece_hash = ?2",
                                params![updated_miners, piece.piece_hash],
                            ).map_err(MetadataDBError::Database)?;

                            // Update miner_pieces
                            debug!("Updating miners for existing piece {}", hex::encode(piece.piece_hash));
                            for miner in &piece.miners {
                                match tx.execute(
                                    "INSERT INTO miner_pieces (miner_uid, piece_hash) VALUES (?1, ?2)",
                                    params![miner.0, piece.piece_hash],
                                ) {
                                    Ok(_) => {
                                        debug!("Inserted miner {} for piece {}", miner.0, hex::encode(piece.piece_hash));
                                    }
                                    Err(e) if e.to_string().contains("UNIQUE constraint failed") => {
                                        // Ignore if miner-piece mapping already exists
                                        debug!("Miner {} for piece {} already exists, skipping insert", miner.0, hex::encode(piece.piece_hash));
                                    }
                                    Err(e) => return Err(MetadataDBError::Database(e)),
                                }
                            }

                        }
                        Err(e) => return Err(MetadataDBError::Database(e)),
                    }

                    // Insert chunk-piece mapping
                    // If it already exists, it will be ignored due to the unique constraint
                    match chunk_pieces_stmt.execute(params![
                        chunk.chunk_hash,
                        piece_idx,
                        piece.piece_hash
                    ]) {
                        Ok(_) => {}
                        Err(e) if e.to_string().contains("UNIQUE constraint failed") => {
                            // Ignore if chunk-piece mapping already exists
                            debug!("Chunk-piece mapping for chunk {} and piece {} already exists, skipping insert", hex::encode(chunk.chunk_hash), hex::encode(piece.piece_hash));
                        }
                        Err(e) => return Err(MetadataDBError::Database(e)),
                    };
                }
            }

            // Drop all prepared statements before committing
            drop(chunk_stmt);
            drop(tracker_chunk_stmt);
            drop(piece_stmt);
            drop(chunk_pieces_stmt);

            tx.commit()?;
            Ok::<(), MetadataDBError>(())
        }).await.map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        // Send result through response channel
        if let Err(e) = response_sender.send(Ok(())).await {
            error!("Failed to send response: {}", e);
            Err(MetadataDBError::Database(
                rusqlite::Error::ExecuteReturnedResults,
            ))
        } else {
            Ok(())
        }
    }

    // a wrapper that  calls MetadatDBCommand::InsertObject
    pub async fn insert_object(
        command_sender: &mpsc::Sender<MetadataDBCommand>,
        infohash_value: InfohashValue,
        nonce: &[u8; 32],
        chunks_with_pieces: Vec<(ChunkValue, Vec<PieceValue>)>,
    ) -> Result<(), MetadataDBError> {
        // Create a channel for the response
        let (response_sender, mut response_receiver) =
            mpsc::channel::<Result<(), MetadataDBError>>(1);

        // First validate the nonce
        let nonce_valid = Self::validate_and_consume_nonce(
            // We need access to the command sender here - this needs to be passed in or restructured
            &(command_sender.clone()),
            infohash_value.owner_account_id.0.clone(),
            nonce,
        )
        .await
        .unwrap_or(false);

        if !nonce_valid {
            if response_sender
                .send(Err(MetadataDBError::InvalidNonce))
                .await
                .is_err()
            {
                error!("Failed to send invalid nonce error response");
            }
            return Ok(());
        }

        // Verify signature after nonce validation
        if !infohash_value.verify_signature(nonce) {
            if response_sender
                .send(Err(MetadataDBError::InvalidSignature))
                .await
                .is_err()
            {
                error!("Failed to send invalid signature error response");
            }
            return Ok(());
        }

        // Send the command to insert the object
        command_sender
            .send(MetadataDBCommand::InsertObject {
                infohash_value,
                chunks_with_pieces,
                response_sender,
            })
            .await
            .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))?;

        // Wait for the response
        match response_receiver.recv().await {
            Some(result) => result,
            None => Err(MetadataDBError::Database(
                rusqlite::Error::ExecuteReturnedResults,
            )),
        }
    }

    pub async fn handle_get_piece(
        &self,
        piece_hash: &PieceHash,
        response_sender: mpsc::Sender<Result<PieceValue, MetadataDBError>>,
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);
        let piece_hash = *piece_hash;

        let result = tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let mut stmt = conn.prepare(
                "SELECT piece_hash, piece_size, piece_type, miners
                 FROM pieces
                 WHERE piece_hash = ?1",
            )?;

            let piece_value = stmt.query_row(params![piece_hash], |row| {
                Ok(PieceValue {
                    piece_hash: row.get(0)?,
                    piece_size: row.get(1)?,
                    piece_type: row.get::<_, u8>(2)?.try_into().map_err(|_| {
                        rusqlite::Error::FromSqlConversionFailure(
                            2,
                            rusqlite::types::Type::Integer,
                            Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "Invalid piece type",
                            )),
                        )
                    })?,
                    miners: serde_json::from_str(row.get::<_, String>(3)?.as_str()).map_err(
                        |e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                3,
                                rusqlite::types::Type::Text,
                                Box::new(e),
                            )
                        },
                    )?,
                })
            })?;

            Ok::<PieceValue, MetadataDBError>(piece_value)
        })
        .await
        .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        // Send the result through the response channel
        if response_sender.send(Ok(result)).await.is_err() {
            error!("Failed to send piece response");
        }

        Ok(())
    }

    pub async fn get_piece(
        command_sender: &mpsc::Sender<MetadataDBCommand>,
        piece_hash: PieceHash,
    ) -> Result<PieceValue, MetadataDBError> {
        // Create a channel for the response
        let (response_sender, mut response_receiver) =
            mpsc::channel::<Result<PieceValue, MetadataDBError>>(1);

        // Send the command to get the piece
        command_sender
            .send(MetadataDBCommand::GetPiece {
                piece_hash,
                response_sender,
            })
            .await
            .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))?;

        // Wait for the response
        match response_receiver.recv().await {
            Some(result) => result,
            None => Err(MetadataDBError::Database(
                rusqlite::Error::ExecuteReturnedResults,
            )),
        }
    }

    pub async fn handle_get_pieces_by_chunk(
        &self,
        chunk_hash: &ChunkHash,
        response_sender: mpsc::Sender<Result<Vec<PieceValue>, MetadataDBError>>,
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);
        let chunk_hash = *chunk_hash;

        let result = tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let mut stmt = conn.prepare(
                "SELECT p.piece_hash, p.piece_size, p.piece_type, p.miners
                    FROM pieces p
                    INNER JOIN chunk_pieces cp ON p.piece_hash = cp.piece_hash
                    WHERE cp.chunk_hash = ?1
                    ORDER BY cp.piece_idx",
            )?;

            let pieces = stmt
                .query_map(params![chunk_hash], |row| {
                    Ok(PieceValue {
                        piece_hash: row.get(0)?,
                        piece_size: row.get(1)?,
                        piece_type: row.get::<_, u8>(2)?.try_into().map_err(|_| {
                            rusqlite::Error::FromSqlConversionFailure(
                                2,
                                rusqlite::types::Type::Integer,
                                Box::new(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "Invalid piece type",
                                )),
                            )
                        })?,
                        miners: serde_json::from_str(row.get::<_, String>(3)?.as_str()).map_err(
                            |e| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    3,
                                    rusqlite::types::Type::Text,
                                    Box::new(e),
                                )
                            },
                        )?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?;

            Ok::<Vec<PieceValue>, MetadataDBError>(pieces)
        })
        .await
        .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        // Send the result through the response channel
        if response_sender.send(Ok(result)).await.is_err() {
            error!("Failed to send pieces response");
        }

        Ok(())
    }

    pub async fn get_pieces_by_chunk(
        command_sender: &mpsc::Sender<MetadataDBCommand>,
        chunk_hash: ChunkHash,
    ) -> Result<Vec<PieceValue>, MetadataDBError> {
        // Create a channel for the response
        let (response_sender, mut response_receiver) =
            mpsc::channel::<Result<Vec<PieceValue>, MetadataDBError>>(1);

        // Send the command to get pieces by chunk
        command_sender
            .send(MetadataDBCommand::GetPiecesByChunk {
                chunk_hash,
                response_sender,
            })
            .await
            .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))?;

        // Wait for the response
        match response_receiver.recv().await {
            Some(result) => result,
            None => Err(MetadataDBError::Database(
                rusqlite::Error::ExecuteReturnedResults,
            )),
        }
    }

    pub async fn handle_get_chunks_by_infohash(
        &self,
        infohash: &[u8],
        response_sender: mpsc::Sender<Result<Vec<ChunkValue>, MetadataDBError>>,
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);
        let infohash = infohash.to_vec();

        let result = tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let mut stmt = conn.prepare(
                "SELECT c.chunk_hash, c.k, c.m, c.chunk_size, c.padlen, c.original_chunk_size
                 FROM tracker_chunks tc
                 JOIN chunks c ON tc.chunk_hash = c.chunk_hash
                 WHERE tc.infohash = ?1
                 ORDER BY tc.chunk_idx",
            )?;

            let chunks: Result<Vec<ChunkValue>, _> = stmt
                .query_map(params![infohash], |row| {
                    let chunk_hash_bytes: Vec<u8> = row.get(0)?;
                    let chunk_hash = chunk_hash_bytes.try_into().map_err(|_| {
                        rusqlite::Error::InvalidColumnType(
                            0,
                            "chunk_hash".to_string(),
                            rusqlite::types::Type::Blob,
                        )
                    })?;

                    Ok(ChunkValue {
                        chunk_hash,
                        k: row.get(1)?,
                        m: row.get(2)?,
                        chunk_size: row.get(3)?,
                        padlen: row.get(4)?,
                        original_chunk_size: row.get(5)?,
                    })
                })?
                .collect();

            chunks.map_err(MetadataDBError::from)
        })
        .await
        .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        // Send result through response channel
        if response_sender.send(Ok(result)).await.is_err() {
            error!("Failed to send chunks response");
        }

        Ok(())
    }

    pub async fn get_chunks_by_infohash(
        command_sender: &mpsc::Sender<MetadataDBCommand>,
        infohash: Vec<u8>,
    ) -> Result<Vec<ChunkValue>, MetadataDBError> {
        // Create a channel for the response
        let (response_sender, mut response_receiver) =
            mpsc::channel::<Result<Vec<ChunkValue>, MetadataDBError>>(1);

        // Send the command to get chunks by infohash
        command_sender
            .send(MetadataDBCommand::GetChunksByInfohash {
                infohash,
                response_sender,
            })
            .await
            .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))?;

        // Wait for the response
        match response_receiver.recv().await {
            Some(result) => result,
            None => Err(MetadataDBError::Database(
                rusqlite::Error::ExecuteReturnedResults,
            )),
        }
    }

    pub async fn handle_get_infohash(
        &self,
        infohash: &InfoHash,
        response_sender: mpsc::Sender<Result<InfohashValue, MetadataDBError>>,
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);
        let infohash = *infohash;

        let result = tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let mut stmt = conn.prepare(
                "SELECT infohash, name, length, chunk_size, chunk_count, owner_account_id, creation_timestamp, signature
                 FROM infohashes
                 WHERE infohash = ?1",
            )?;

            let infohash_value = stmt.query_row(params![infohash], |row| {
                Ok(InfohashValue {
                    infohash: row.get(0)?,
                    name: row.get(1)?,
                    length: row.get(2)?,
                    chunk_size: row.get(3)?,
                    chunk_count: row.get(4)?,
                    owner_account_id: row.get::<_, SqlAccountId>(5)?,
                    creation_timestamp: row.get::<_, SqlDateTime>(6)?.0,
                    signature: {
                        let vec = row.get::<_, Vec<u8>>(7)?;
                        let arr: [u8; 64] = vec.try_into().map_err(|_| {
                            rusqlite::Error::FromSqlConversionFailure(
                                7,
                                rusqlite::types::Type::Blob,
                                Box::new(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "Invalid signature length",
                                )),
                            )
                        })?;
                        KeypairSignature::from_raw(arr)
                    },
                })
            })?;

            Ok::<InfohashValue, MetadataDBError>(infohash_value)
        })
        .await
        .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        // Send result through response channel
        if response_sender.send(Ok(result)).await.is_err() {
            error!("Failed to send infohash response");
        }

        Ok(())
    }

    pub async fn get_infohash(
        command_sender: &mpsc::Sender<MetadataDBCommand>,
        infohash: InfoHash,
    ) -> Result<InfohashValue, MetadataDBError> {
        // Create a channel for the response
        let (response_sender, mut response_receiver) =
            mpsc::channel::<Result<InfohashValue, MetadataDBError>>(1);

        // Send the command to get infohash
        command_sender
            .send(MetadataDBCommand::GetInfohash {
                infohash,
                response_sender,
            })
            .await
            .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))?;

        // Wait for the response
        match response_receiver.recv().await {
            Some(result) => result,
            None => Err(MetadataDBError::Database(
                rusqlite::Error::ExecuteReturnedResults,
            )),
        }
    }

    pub async fn handle_get_random_chunk(
        &self,
        response_sender: mpsc::Sender<Result<ChunkValue, MetadataDBError>>,
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);

        let result = tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let mut stmt = conn.prepare(
                "SELECT chunk_hash, k, m, chunk_size, padlen, original_chunk_size
                 FROM chunks
                 ORDER BY RANDOM() LIMIT 1",
            )?;

            // Execute the query and map the result to ChunkValue
            let chunk = stmt.query_row([], |row| {
                let chunk_hash_bytes: Vec<u8> = row.get(0)?;
                let chunk_hash = chunk_hash_bytes.try_into().map_err(|_| {
                    rusqlite::Error::InvalidColumnType(
                        0,
                        "chunk_hash".to_string(),
                        rusqlite::types::Type::Blob,
                    )
                })?;

                Ok(ChunkValue {
                    chunk_hash,
                    k: row.get(1)?,
                    m: row.get(2)?,
                    chunk_size: row.get(3)?,
                    padlen: row.get(4)?,
                    original_chunk_size: row.get(5)?,
                })
            })?;

            Ok::<ChunkValue, MetadataDBError>(chunk)
        })
        .await
        .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        // Send the result through the response channel
        if response_sender.send(Ok(result)).await.is_err() {
            error!("Failed to send random chunk response");
        }

        Ok(())
    }

    pub async fn get_random_chunk(
        command_sender: &mpsc::Sender<MetadataDBCommand>,
    ) -> Result<ChunkValue, MetadataDBError> {
        // Create a channel for the response
        let (response_sender, mut response_receiver) =
            mpsc::channel::<Result<ChunkValue, MetadataDBError>>(1);

        // Send the command to get a random chunk
        command_sender
            .send(MetadataDBCommand::GetRandomChunk { response_sender })
            .await
            .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))?;

        // Wait for the response
        match response_receiver.recv().await {
            Some(result) => result,
            None => Err(MetadataDBError::Database(
                rusqlite::Error::ExecuteReturnedResults,
            )),
        }
    }

    pub async fn handle_get_piece_repair_history(
        &self,
        piece_repair_hash: &[u8; 32],
        response_sender: mpsc::Sender<Result<PieceChallengeHistory, MetadataDBError>>,
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);
        let piece_repair_hash = *piece_repair_hash;

        let result = tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let mut stmt = conn.prepare(
                "SELECT piece_repair_hash, piece_hash, chunk_hash, validator_id, timestamp, signature
                 FROM piece_repair_history
                 WHERE piece_repair_hash = ?1",
            )?;

            let piece_challenge_history = stmt.query_row(params![piece_repair_hash], |row| {
                Ok(PieceChallengeHistory {
                    piece_repair_hash: row.get(0)?,
                    piece_hash: row.get(1)?,
                    chunk_hash: row.get(2)?,
                    validator_id: Compact(row.get::<_, u16>(3)?),
                    timestamp: row.get::<_, SqlDateTime>(4)?.0,
                    signature: {
                        let vec = row.get::<_, Vec<u8>>(5)?;
                        let arr: [u8; 64] = vec.try_into().map_err(|_| {
                            rusqlite::Error::FromSqlConversionFailure(
                                5,
                                rusqlite::types::Type::Blob,
                                Box::new(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "Invalid signature length",
                                )),
                            )
                        })?;
                        KeypairSignature::from_raw(arr)
                    },
                })
            })?;

            Ok::<PieceChallengeHistory, MetadataDBError>(piece_challenge_history)
        })
        .await
        .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        // Send result through response channel
        if response_sender.send(Ok(result)).await.is_err() {
            error!("Failed to send piece repair history response");
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub async fn get_piece_repair_history(
        command_sender: &mpsc::Sender<MetadataDBCommand>,
        piece_repair_hash: [u8; 32],
    ) -> Result<PieceChallengeHistory, MetadataDBError> {
        // Create a channel for the response
        let (response_sender, mut response_receiver) =
            mpsc::channel::<Result<PieceChallengeHistory, MetadataDBError>>(1);

        // Send the command to get piece repair history
        command_sender
            .send(MetadataDBCommand::GetPieceRepairHistory {
                piece_repair_hash,
                response_sender,
            })
            .await
            .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))?;

        // Wait for the response
        match response_receiver.recv().await {
            Some(result) => result,
            None => Err(MetadataDBError::Database(
                rusqlite::Error::ExecuteReturnedResults,
            )),
        }
    }

    pub async fn handle_get_chunk_challenge_history(
        &self,
        challenge_hash: &[u8; 32],
        response_sender: mpsc::Sender<Result<ChunkChallengeHistory, MetadataDBError>>,
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);
        let challenge_hash = *challenge_hash;

        let result = tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let mut stmt = conn.prepare(
                "SELECT challenge_hash, chunk_hash, validator_id, miners_challenged, miners_successful, piece_repair_hash, timestamp, signature
                 FROM chunk_challenge_history
                 WHERE challenge_hash = ?1",
            )?;

            let chunk_challenge_history =
                stmt.query_row(params![challenge_hash], |row| {
                    Ok(ChunkChallengeHistory {
                        challenge_hash: row.get(0)?,
                        chunk_hash: row.get(1)?,
                        validator_id: Compact(row.get::<_, u16>(2)?),
                        miners_challenged: serde_json::from_str(&row.get::<_, String>(3)?)
                            .map_err(|e| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    3,
                                    rusqlite::types::Type::Text,
                                    Box::new(e),
                                )
                            })?,
                        miners_successful: serde_json::from_str(&row.get::<_, String>(4)?)
                            .map_err(|e| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    4,
                                    rusqlite::types::Type::Text,
                                    Box::new(e),
                                )
                            })?,
                        piece_repair_hash: row.get(5)?,
                        timestamp: row.get::<_, SqlDateTime>(6)?.0,
                        signature: {
                            let vec = row.get::<_, Vec<u8>>(7)?;
                            let arr: [u8; 64] = vec.try_into().map_err(|_| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    7,
                                    rusqlite::types::Type::Blob,
                                    Box::new(std::io::Error::new(
                                        std::io::ErrorKind::InvalidData,
                                        "Invalid signature length",
                                    )),
                                )
                            })?;
                            KeypairSignature::from_raw(arr)
                        },
                    })
                })?;

            Ok::<ChunkChallengeHistory, MetadataDBError>(chunk_challenge_history)
        })
        .await
        .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        // Send result through response channel
        if response_sender.send(Ok(result)).await.is_err() {
            error!("Failed to send chunk challenge history response");
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub async fn get_chunk_challenge_history(
        command_sender: &mpsc::Sender<MetadataDBCommand>,
        challenge_hash: [u8; 32],
    ) -> Result<ChunkChallengeHistory, MetadataDBError> {
        // Create a channel for the response
        let (response_sender, mut response_receiver) =
            mpsc::channel::<Result<ChunkChallengeHistory, MetadataDBError>>(1);

        // Send the command to get chunk challenge history
        command_sender
            .send(MetadataDBCommand::GetChunkChallengeHistory {
                challenge_hash,
                response_sender,
            })
            .await
            .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))?;

        // Wait for the response
        match response_receiver.recv().await {
            Some(result) => result,
            None => Err(MetadataDBError::Database(
                rusqlite::Error::ExecuteReturnedResults,
            )),
        }
    }

    pub async fn handle_delete_infohash(
        &self,
        infohash_value: InfohashValue,
        response_sender: mpsc::Sender<Result<(), MetadataDBError>>,
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);
        let infohash_value = infohash_value.clone();

        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let tx = conn.unchecked_transaction()?;

            // First, update the ref counts for chunks and pieces BEFORE deleting tracker_chunks
            let mut chunk_stmt = tx.prepare(
                "UPDATE chunks SET ref_count = ref_count - 1 WHERE chunk_hash IN (
                    SELECT chunk_hash FROM tracker_chunks WHERE infohash = ?1
                )",
            )?;
            chunk_stmt
                .execute(params![infohash_value.infohash])
                .map_err(MetadataDBError::Database)?;
            drop(chunk_stmt);

            let mut piece_stmt = tx.prepare(
                "UPDATE pieces SET ref_count = ref_count - 1 WHERE piece_hash IN (
                    SELECT piece_hash FROM chunk_pieces WHERE chunk_hash IN (
                        SELECT chunk_hash FROM tracker_chunks WHERE infohash = ?1
                    )
                )",
            )?;
            piece_stmt
                .execute(params![infohash_value.infohash])
                .map_err(MetadataDBError::Database)?;
            drop(piece_stmt);

            // Now delete the tracker chunks associated with this infohash
            let mut tracker_stmt = tx.prepare("DELETE FROM tracker_chunks WHERE infohash = ?1")?;
            tracker_stmt
                .execute(params![infohash_value.infohash])
                .map_err(MetadataDBError::Database)?;
            drop(tracker_stmt);

            // Delete the infohash entry from the database
            let mut stmt =
                tx.prepare("DELETE FROM infohashes WHERE infohash = ?1 AND owner_account_id = ?2")?;
            stmt.execute(params![
                infohash_value.infohash,
                infohash_value.owner_account_id.clone()
            ])
            .map_err(MetadataDBError::Database)?;
            drop(stmt);

            // Delete chunks and pieces, and chunk_pieces with ref_count <= 0
            let mut delete_chunks_stmt = tx.prepare("DELETE FROM chunks WHERE ref_count <= 0")?;
            delete_chunks_stmt
                .execute([])
                .map_err(MetadataDBError::Database)?;
            drop(delete_chunks_stmt);

            let mut delete_pieces_stmt = tx.prepare("DELETE FROM pieces WHERE ref_count <= 0")?;
            delete_pieces_stmt
                .execute([])
                .map_err(MetadataDBError::Database)?;
            drop(delete_pieces_stmt);

            let mut delete_chunk_pieces_stmt = tx.prepare(
                "DELETE FROM chunk_pieces WHERE piece_hash NOT IN (SELECT piece_hash FROM pieces)",
            )?;
            delete_chunk_pieces_stmt
                .execute([])
                .map_err(MetadataDBError::Database)?;
            drop(delete_chunk_pieces_stmt);

            // Commit the transaction
            tx.commit().map_err(MetadataDBError::Database)?;

            Ok::<(), MetadataDBError>(())
        })
        .await
        .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        // Send the response
        if response_sender.send(Ok(())).await.is_err() {
            error!("Failed to send delete infohash response");
        }
        debug!(
            "Successfully deleted infohash: {}",
            hex::encode(infohash_value.infohash)
        );

        Ok(())
    }

    pub async fn delete_infohash(
        command_sender: &mpsc::Sender<MetadataDBCommand>,
        infohash_value: InfohashValue,
        nonce: &[u8; 32],
    ) -> Result<(), MetadataDBError> {
        // Create a channel for the response
        let (response_sender, mut response_receiver) =
            mpsc::channel::<Result<(), MetadataDBError>>(1);

        // First validate the nonce
        let nonce_valid = Self::validate_and_consume_nonce(
            // We need access to the command sender here - this needs to be passed in or restructured
            &(command_sender.clone()),
            infohash_value.owner_account_id.0.clone(),
            nonce,
        )
        .await
        .unwrap_or(false);

        if !nonce_valid {
            if response_sender
                .send(Err(MetadataDBError::InvalidNonce))
                .await
                .is_err()
            {
                error!("Failed to send invalid nonce error response");
            }
            return Ok(());
        }

        // Verify signature after nonce validation
        if !infohash_value.verify_signature(nonce) {
            if response_sender
                .send(Err(MetadataDBError::InvalidSignature))
                .await
                .is_err()
            {
                error!("Failed to send invalid signature error response");
            }
            return Ok(());
        }

        // Send the command to delete the infohash
        command_sender
            .send(MetadataDBCommand::DeleteInfohash {
                infohash_value,
                response_sender,
            })
            .await
            .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))?;

        // Wait for the response
        match response_receiver.recv().await {
            Some(result) => result,
            None => Err(MetadataDBError::Database(
                rusqlite::Error::ExecuteReturnedResults,
            )),
        }
    }

    // Add handlers for the new commands
    pub async fn handle_get_infohashes_by_owner(
        &self,
        owner_account_id: &AccountId,
        response_sender: mpsc::Sender<Result<Vec<InfohashValue>, MetadataDBError>>,
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);
        let owner_account_id = SqlAccountId(owner_account_id.clone());

        let result = tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let mut stmt = conn.prepare(
                "SELECT infohash, name, length, chunk_size, chunk_count, owner_account_id, creation_timestamp, signature FROM infohashes WHERE owner_account_id = ?1"
            )?;

            let infohashes: Result<Vec<InfohashValue>, _> = stmt.query_map(
                params![owner_account_id],
                |row| {
                    Ok(InfohashValue {
                        infohash: row.get(0)?,
                        name: row.get(1)?,
                        length: row.get(2)?,
                        chunk_size: row.get(3)?,
                        chunk_count: row.get(4)?,
                        owner_account_id: row.get(5)?,
                        creation_timestamp: row.get::<_, SqlDateTime>(6)?.0,
                        signature: KeypairSignature::from_raw({
                            let sig_bytes: Vec<u8> = row.get(7)?;
                            let mut sig_array = [0u8; 64];
                            if sig_bytes.len() == 64 {
                                sig_array.copy_from_slice(&sig_bytes);
                            }
                            sig_array
                        }),
                    })
                }
            )?.collect();

            infohashes.map_err(MetadataDBError::from)
        })
        .await
        .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        if response_sender.send(Ok(result)).await.is_err() {
            error!("Failed to send get infohashes by owner response");
        }
        Ok(())
    }

    pub async fn handle_verify_ownership(
        &self,
        infohash: &InfoHash,
        owner_account_id: &AccountId,
        response_sender: mpsc::Sender<Result<bool, MetadataDBError>>,
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);
        let infohash = *infohash;
        let owner_account_id = owner_account_id.clone();

        let result = tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let owner_check: Result<SqlAccountId, _> = conn.query_row(
                "SELECT owner_account_id FROM infohashes WHERE infohash = ?1",
                params![infohash],
                |row| row.get(0),
            );

            match owner_check {
                Ok(owner) => Ok::<bool, MetadataDBError>(owner.0 == owner_account_id),
                Err(_) => Ok::<bool, MetadataDBError>(false),
            }
        })
        .await
        .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        if response_sender.send(Ok(result)).await.is_err() {
            error!("Failed to send verify ownership response");
        }
        Ok(())
    }

    /// Processes db events and commands.
    pub async fn process_events(&mut self) {
        while let Some(command) = self.command_receiver.recv().await {
            match command {
                MetadataDBCommand::GetDBVersion { response_sender } => {
                    debug!("Handling request for DB version");
                    if let Err(e) = self.handle_get_db_version(response_sender).await {
                        error!("Error retrieving DB version: {:?}", e);
                    }
                }
                MetadataDBCommand::GetSiteId { response_sender } => {
                    debug!("Handling request for site ID");
                    if let Err(e) = self.handle_get_site_id(response_sender).await {
                        error!("Error retrieving site ID: {:?}", e);
                    }
                }
                MetadataDBCommand::InsertCrSqliteChanges {
                    changes,
                    response_sender,
                } => {
                    // Small preview of changes
                    debug!(
                        "Inserting crsqlite changes: {:?}...",
                        changes.iter().take(3).collect::<Vec<_>>()
                    );
                    if let Err(e) = self
                        .handle_insert_crsqlite_changes(changes, response_sender)
                        .await
                    {
                        error!("Error inserting crsqlite changes: {:?}", e);
                    }
                }
                MetadataDBCommand::GetCrSqliteChanges {
                    min_db_version,
                    site_id_exclude,
                    response_sender,
                } => {
                    debug!("Handling request for crsqlite changes");
                    if let Err(e) = self
                        .handle_get_crsqlite_changes(
                            min_db_version,
                            site_id_exclude,
                            response_sender,
                        )
                        .await
                    {
                        error!("Error retrieving crsqlite changes: {:?}", e);
                    }
                }
                MetadataDBCommand::InsertObject {
                    infohash_value,
                    chunks_with_pieces,
                    response_sender,
                } => {
                    debug!(
                        "Inserting object with infohash: {:?}, chunks: {}",
                        infohash_value.infohash,
                        chunks_with_pieces.len()
                    );
                    if let Err(e) = self
                        .handle_insert_object(&infohash_value, chunks_with_pieces, response_sender)
                        .await
                    {
                        error!("Error inserting object: {:?}", e);
                    }
                }
                MetadataDBCommand::DeleteInfohash {
                    infohash_value,
                    response_sender,
                } => {
                    debug!("Handling delete infohash request");
                    if let Err(e) = self
                        .handle_delete_infohash(infohash_value, response_sender)
                        .await
                    {
                        error!("Error deleting infohash: {:?}", e);
                    }
                }
                MetadataDBCommand::GetRandomChunk { response_sender } => {
                    debug!("Handling request for random chunk");
                    if let Err(e) = self.handle_get_random_chunk(response_sender).await {
                        error!("Error retrieving random chunk: {:?}", e);
                    }
                }
                MetadataDBCommand::QueuePiecesForRepair {
                    miner_uid,
                    response_sender,
                } => {
                    debug!("Handling queue pieces for repair request");
                    if let Err(e) = self
                        .handle_queue_pieces_for_repair(miner_uid, response_sender)
                        .await
                    {
                        error!("Error queuing pieces for repair: {:?}", e);
                    }
                }
                MetadataDBCommand::GetPiecesForRepair { response_sender } => {
                    debug!("Handling get pieces for repair request");
                    if let Err(e) = self.handle_get_pieces_for_repair(response_sender).await {
                        error!("Error getting pieces for repair: {:?}", e);
                    }
                }
                MetadataDBCommand::InsertPieceRepairHistory {
                    piece_repair_history,
                    response_sender,
                } => {
                    debug!("Handling insert piece repair history");
                    if let Err(e) = self
                        .handle_insert_piece_repair_history(&piece_repair_history, response_sender)
                        .await
                    {
                        error!("Error inserting piece repair history: {:?}", e);
                    }
                }
                MetadataDBCommand::InsertChunkChallengeHistory {
                    chunk_challenge_history,
                    response_sender,
                } => {
                    debug!("Handling insert chunk challenge history");
                    if let Err(e) = self
                        .handle_insert_chunk_challenge_history(
                            &chunk_challenge_history,
                            response_sender,
                        )
                        .await
                    {
                        error!("Error inserting chunk challenge history: {:?}", e);
                    }
                }
                MetadataDBCommand::GetPiece {
                    piece_hash,
                    response_sender,
                } => {
                    debug!("Handling get piece");
                    if let Err(e) = self.handle_get_piece(&piece_hash, response_sender).await {
                        error!("Error getting piece: {:?}", e);
                    }
                }
                MetadataDBCommand::GetPiecesByChunk {
                    chunk_hash,
                    response_sender,
                } => {
                    debug!("Handling get pieces by chunk");
                    if let Err(e) = self
                        .handle_get_pieces_by_chunk(&chunk_hash, response_sender)
                        .await
                    {
                        error!("Error getting pieces by chunk: {:?}", e);
                    }
                }
                MetadataDBCommand::GetChunksByInfohash {
                    infohash,
                    response_sender,
                } => {
                    debug!("Handling get chunks by infohash");
                    if let Err(e) = self
                        .handle_get_chunks_by_infohash(&infohash, response_sender)
                        .await
                    {
                        error!("Error getting chunks by infohash: {:?}", e);
                    }
                }
                MetadataDBCommand::GetInfohash {
                    infohash,
                    response_sender,
                } => {
                    debug!("Handling get infohash");
                    if let Err(e) = self.handle_get_infohash(&infohash, response_sender).await {
                        error!("Error getting infohash: {:?}", e);
                    }
                }
                MetadataDBCommand::GetPieceRepairHistory {
                    piece_repair_hash,
                    response_sender,
                } => {
                    debug!("Handling get piece repair history");
                    if let Err(e) = self
                        .handle_get_piece_repair_history(&piece_repair_hash, response_sender)
                        .await
                    {
                        error!("Error getting piece repair history: {:?}", e);
                    }
                }
                MetadataDBCommand::GetChunkChallengeHistory {
                    challenge_hash,
                    response_sender,
                } => {
                    debug!("Handling get chunk challenge history");
                    if let Err(e) = self
                        .handle_get_chunk_challenge_history(&challenge_hash, response_sender)
                        .await
                    {
                        error!("Error getting chunk challenge history: {:?}", e);
                    }
                }
                MetadataDBCommand::GetInfohashesByOwner {
                    owner_account_id,
                    response_sender,
                } => {
                    debug!("Handling get infohashes by owner request");
                    if let Err(e) = self
                        .handle_get_infohashes_by_owner(&owner_account_id, response_sender)
                        .await
                    {
                        error!("Error getting infohashes by owner: {:?}", e);
                    }
                }
                MetadataDBCommand::VerifyOwnership {
                    infohash,
                    owner_account_id,
                    response_sender,
                } => {
                    debug!("Handling verify ownership request");
                    if let Err(e) = self
                        .handle_verify_ownership(&infohash, &owner_account_id, response_sender)
                        .await
                    {
                        error!("Error verifying ownership: {:?}", e);
                    }
                }
                MetadataDBCommand::GetNonce {
                    account_id,
                    response_sender,
                } => {
                    debug!("Handling get nonce request");
                    if let Err(e) = self.handle_get_nonce(&account_id, response_sender).await {
                        error!("Error getting nonce: {:?}", e);
                    }
                }
                MetadataDBCommand::GenerateNonce {
                    account_id,
                    response_sender,
                } => {
                    debug!("Handling generate nonce request");
                    if let Err(e) = self
                        .handle_generate_nonce(&account_id, response_sender)
                        .await
                    {
                        error!("Error generating nonce: {:?}", e);
                    }
                }
                MetadataDBCommand::ValidateAndConsumeNonce {
                    account_id,
                    nonce,
                    response_sender,
                } => {
                    debug!("Handling validate nonce request");
                    if let Err(e) = self
                        .handle_validate_and_consume_nonce(&account_id, &nonce, response_sender)
                        .await
                    {
                        error!("Error validating nonce: {:?}", e);
                    }
                }
                MetadataDBCommand::CleanupExpiredNonces {
                    max_age,
                    response_sender,
                } => {
                    debug!("Handling cleanup expired nonces request");
                    if let Err(e) = self
                        .handle_cleanup_expired_nonces(max_age, response_sender)
                        .await
                    {
                        error!("Error cleaning up expired nonces: {:?}", e);
                    }
                }
            }
        }
    }
}

// tests for insertion and query functions in the metadatadb
#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use base::piece::PieceType;
    use chrono::Utc;
    use crabtensor::{sign::sign_message, wallet::signer_from_seed};
    use subxt::ext::codec::Compact;
    use tempfile::NamedTempFile;

    use super::*;
    use crate::metadata::models::{ChunkValue, InfohashValue, PieceValue};

    // Helper function to create a test database with schema
    fn setup_test_db(temp_file: &NamedTempFile) -> PathBuf {
        let db_path = temp_file.path().to_path_buf();

        // Path to the CRSQLite extension
        let crsqlite_lib = PathBuf::from("../../crsqlite/crsqlite.so");

        // Create connection and set up schema
        let conn = Connection::open(&db_path).expect("Failed to open database");

        // Load extension
        unsafe {
            conn.load_extension_enable()
                .expect("Failed to enable extensions");
            conn.load_extension(&crsqlite_lib, Some("sqlite3_crsqlite_init"))
                .expect("Failed to load extension");
            conn.load_extension_disable()
                .expect("Failed to disable extensions");
        }

        // Create tables matching the migration schema
        conn.execute(
            "CREATE TABLE infohashes (
                infohash BLOB PRIMARY KEY NOT NULL DEFAULT '',
                name VARCHAR(4096) NOT NULL DEFAULT 'default',
                length INTEGER NOT NULL DEFAULT 0,
                chunk_size INTEGER NOT NULL DEFAULT 0,
                chunk_count INTEGER NOT NULL DEFAULT 0,
                owner_account_id BLOB NOT NULL DEFAULT '',
                creation_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                signature BLOB NOT NULL DEFAULT ''
            )",
            [],
        )
        .expect("Failed to create infohashes table");

        conn.execute(
            "CREATE TABLE chunks (
                chunk_hash BLOB PRIMARY KEY NOT NULL DEFAULT '',
                k INTEGER NOT NULL DEFAULT 0,
                m INTEGER NOT NULL DEFAULT 0,
                chunk_size INTEGER NOT NULL DEFAULT 0,
                padlen INTEGER NOT NULL DEFAULT 0,
                original_chunk_size INTEGER NOT NULL DEFAULT 0,
                ref_count INTEGER NOT NULL DEFAULT 1
            )",
            [],
        )
        .expect("Failed to create chunks table");

        conn.execute(
            "CREATE TABLE tracker_chunks (
                infohash BLOB NOT NULL DEFAULT '',
                chunk_idx INTEGER NOT NULL DEFAULT 0,
                chunk_hash BLOB NOT NULL DEFAULT '',
                PRIMARY KEY (infohash, chunk_idx)
            )",
            [],
        )
        .expect("Failed to create tracker_chunks table");

        conn.execute(
            "CREATE TABLE pieces (
                piece_hash BLOB PRIMARY KEY NOT NULL DEFAULT '',
                piece_size INTEGER NOT NULL DEFAULT 0,
                piece_type INTEGER NOT NULL DEFAULT 0,
                miners TEXT NOT NULL DEFAULT '',
                ref_count INTEGER NOT NULL DEFAULT 1
            )",
            [],
        )
        .expect("Failed to create pieces table");

        conn.execute(
            "CREATE TABLE chunk_pieces (
                chunk_hash BLOB NOT NULL DEFAULT '',
                piece_idx INTEGER NOT NULL DEFAULT 0,
                piece_hash BLOB NOT NULL DEFAULT '',
                PRIMARY KEY (chunk_hash, piece_idx)
            )",
            [],
        )
        .expect("Failed to create chunk_pieces table");

        conn.execute(
            "CREATE TABLE piece_repair_history (
                piece_repair_hash BLOB PRIMARY KEY NOT NULL DEFAULT '',
                piece_hash BLOB NOT NULL DEFAULT '',
                chunk_hash BLOB NOT NULL DEFAULT '',
                validator_id INTEGER NOT NULL DEFAULT 0,
                timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                signature BLOB NOT NULL DEFAULT ''
            )",
            [],
        )
        .expect("Failed to create piece_repair_history table");

        conn.execute(
            "CREATE TABLE chunk_challenge_history (
                challenge_hash BLOB PRIMARY KEY NOT NULL DEFAULT '',
                chunk_hash BLOB NOT NULL DEFAULT '',
                validator_id INTEGER NOT NULL DEFAULT 0,
                miners_challenged TEXT NOT NULL DEFAULT '',
                miners_successful TEXT NOT NULL DEFAULT '',
                piece_repair_hash BLOB,
                timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                signature BLOB NOT NULL DEFAULT ''
            )",
            [],
        )
        .expect("Failed to create chunk_challenge_history table");

        conn.execute(
            "CREATE TABLE account_nonces (
                account_id BLOB NOT NULL,
                nonce BLOB NOT NULL,
                timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (account_id, nonce)
            )",
            [],
        )
        .expect("Failed to create account_nonces table");

        // Create indexes matching the migration
        conn.execute("CREATE INDEX idx_chunks_by_hash ON chunks(chunk_hash)", [])
            .expect("Failed to create chunks index");
        conn.execute("CREATE INDEX idx_pieces_by_hash ON pieces(piece_hash)", [])
            .expect("Failed to create pieces index");
        conn.execute("CREATE INDEX idx_chunks_ref_count ON chunks(ref_count)", [])
            .expect("Failed to create chunks ref_count index");
        conn.execute("CREATE INDEX idx_pieces_ref_count ON pieces(ref_count)", [])
            .expect("Failed to create pieces ref_count index");
        conn.execute(
            "CREATE INDEX idx_chunk_pieces_by_chunk ON chunk_pieces(chunk_hash)",
            [],
        )
        .expect("Failed to create chunk_pieces index");
        conn.execute(
            "CREATE INDEX idx_tracker_chunks_by_infohash ON tracker_chunks(infohash)",
            [],
        )
        .expect("Failed to create tracker_chunks index");
        conn.execute(
            "CREATE INDEX idx_infohashes_by_owner ON infohashes(owner_account_id)",
            [],
        )
        .expect("Failed to create infohashes owner index");
        conn.execute(
            "CREATE INDEX idx_piece_repair_by_piece ON piece_repair_history(piece_hash)",
            [],
        )
        .expect("Failed to create piece_repair_history index");
        conn.execute(
            "CREATE INDEX idx_chunk_challenge_by_chunk ON chunk_challenge_history(chunk_hash)",
            [],
        )
        .expect("Failed to create chunk_challenge_history index");
        conn.execute(
            "CREATE INDEX idx_account_nonces_timestamp ON account_nonces(timestamp)",
            [],
        )
        .expect("Failed to create account_nonces timestamp index");

        // Return the database path
        db_path
    }
    #[tokio::test]
    async fn test_insert_and_query_object() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let db_path = setup_test_db(&temp_file);
        let crsqlite_lib = PathBuf::from("../../crsqlite/crsqlite.so");

        let (mut db, command_sender) = MetadataDB::new(&db_path, &crsqlite_lib).unwrap();

        // Start the DB event processing in the background
        let handle = tokio::spawn(async move {
            db.process_events().await;
        });

        // Create test data
        let infohash = InfoHash([1u8; 32]);

        // Create dummy account
        let decoded =
            hex::decode("ba4f8ae740dd5f7950081049e32f6ed071e3dd3d8d6f3c5073caf22924569062")
                .unwrap();
        // Create dummy account
        let test_phrase: &[u8] = decoded.as_slice();
        let signer = signer_from_seed(test_phrase).expect("Failed to create signer from seed");
        let dummy_account_id = signer.account_id().clone();

        // generate nonce for the account
        let nonce = MetadataDB::generate_nonce(&command_sender, dummy_account_id.clone())
            .await
            .expect("Failed to generate nonce");

        // sign the infohash value details with the dummy account
        let infohash_value = InfohashValue {
            infohash: InfoHash([1; 32]),
            name: "test_file.txt".to_string(),
            length: 1000,
            chunk_size: 256,
            chunk_count: 4,
            owner_account_id: SqlAccountId(dummy_account_id),
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0; 64]),
        };

        // Sign the infohash value
        let message = infohash_value.get_signature_message(&nonce);
        let signature = sign_message(&signer, &message);
        let infohash_value = InfohashValue {
            signature, // Use the signed signature
            ..infohash_value
        };

        let chunk = ChunkValue {
            chunk_hash: ChunkHash([2u8; 32]),
            k: 10,
            m: 5,
            chunk_size: 512,
            padlen: 0,
            original_chunk_size: 512,
        };

        let piece = PieceValue {
            piece_hash: PieceHash([3u8; 32]),
            piece_size: 256,
            piece_type: PieceType::Data,
            miners: vec![Compact(1), Compact(2)],
        };
        let piece_clone = piece.clone();

        // Insert the object and wait for it to complete
        MetadataDB::insert_object(
            &command_sender,
            infohash_value.clone(),
            &nonce,
            vec![(chunk.clone(), vec![piece])],
        )
        .await
        .expect("Failed to insert object");

        // Query the infohash
        let queried_infohash = MetadataDB::get_infohash(&command_sender, infohash)
            .await
            .expect("Failed to get infohash");
        assert_eq!(queried_infohash.infohash, infohash);
        assert_eq!(queried_infohash.length, 1000);
        assert_eq!(queried_infohash.chunk_size, 256);
        assert_eq!(queried_infohash.chunk_count, 4);

        // Query the chunks by infohash
        let queried_chunks = MetadataDB::get_chunks_by_infohash(&command_sender, infohash.to_vec())
            .await
            .expect("Failed to get chunks by infohash");
        assert_eq!(queried_chunks.len(), 1);
        assert_eq!(queried_chunks[0].chunk_hash, chunk.chunk_hash);
        assert_eq!(queried_chunks[0].k, chunk.k);
        assert_eq!(queried_chunks[0].m, chunk.m);
        assert_eq!(queried_chunks[0].chunk_size, chunk.chunk_size);
        assert_eq!(queried_chunks[0].padlen, chunk.padlen);
        assert_eq!(
            queried_chunks[0].original_chunk_size,
            chunk.original_chunk_size
        );

        // Query the pieces by chunk
        let queried_pieces = MetadataDB::get_pieces_by_chunk(&command_sender, chunk.chunk_hash)
            .await
            .expect("Failed to get pieces by chunk");
        assert_eq!(queried_pieces.len(), 1);
        assert_eq!(queried_pieces[0].piece_hash, piece_clone.piece_hash);
        assert_eq!(queried_pieces[0].piece_size, piece_clone.piece_size);
        assert_eq!(queried_pieces[0].miners, piece_clone.miners);

        // Abort the background task
        handle.abort();
        // temp_file will be automatically cleaned up when it goes out of scope
    }

    #[tokio::test]
    async fn test_insert_object_and_get_random_chunk() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let db_path = setup_test_db(&temp_file);
        let crsqlite_lib = PathBuf::from("../../crsqlite/crsqlite.so");

        let (mut db, command_sender) = MetadataDB::new(&db_path, &crsqlite_lib).unwrap();

        // Start the DB event processing in the background
        let handle = tokio::spawn(async move {
            db.process_events().await;
        });

        // Create dummy account
        let decoded =
            hex::decode("ba4f8ae740dd5f7950081049e32f6ed071e3dd3d8d6f3c5073caf22924569062")
                .unwrap();
        // Create dummy account
        let test_phrase: &[u8] = decoded.as_slice();
        let signer = signer_from_seed(test_phrase).expect("Failed to create signer from seed");
        let dummy_account_id = SqlAccountId(signer.account_id().clone());

        // generate nonce for the account
        let nonce = MetadataDB::generate_nonce(&command_sender, dummy_account_id.clone().0)
            .await
            .expect("Failed to generate nonce");

        let infohash_value = InfohashValue {
            infohash: InfoHash([1; 32]),
            name: "test_file.txt".to_string(),
            length: 1000,
            chunk_size: 256,
            chunk_count: 4,
            owner_account_id: dummy_account_id,
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0; 64]),
        };

        // Sign the infohash value
        let message = infohash_value.get_signature_message(&nonce);
        let signature = sign_message(&signer, &message);
        let infohash_value = InfohashValue {
            signature, // Use the signed signature
            ..infohash_value
        };

        let chunk = ChunkValue {
            chunk_hash: ChunkHash([2u8; 32]),
            k: 10,
            m: 5,
            chunk_size: 512,
            padlen: 0,
            original_chunk_size: 512,
        };

        let piece = PieceValue {
            piece_hash: PieceHash([3u8; 32]),
            piece_size: 256,
            piece_type: PieceType::Data,
            miners: vec![Compact(1), Compact(2)],
        };

        // Insert the object
        MetadataDB::insert_object(
            &command_sender,
            infohash_value,
            &nonce,
            vec![(chunk, vec![piece])],
        )
        .await
        .expect("Failed to insert object");

        // Query a random chunk
        let random_chunk = MetadataDB::get_random_chunk(&command_sender)
            .await
            .expect("Failed to get random chunk");
        assert_eq!(random_chunk.chunk_hash, ChunkHash([2u8; 32]));

        // Abort the background task
        handle.abort();
        // temp_file will be automatically cleaned up when it goes out of scope
    }

    #[tokio::test]
    async fn test_insert_object_get_piece() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let db_path = setup_test_db(&temp_file);
        let crsqlite_lib = PathBuf::from("../../crsqlite/crsqlite.so");

        let (mut db, command_sender) = MetadataDB::new(&db_path, &crsqlite_lib).unwrap();

        // Start the DB event processing in the background
        let handle = tokio::spawn(async move {
            db.process_events().await;
        });

        let decoded =
            hex::decode("ba4f8ae740dd5f7950081049e32f6ed071e3dd3d8d6f3c5073caf22924569062")
                .unwrap();
        // Create dummy account
        let test_phrase: &[u8] = decoded.as_slice();
        let signer = signer_from_seed(test_phrase).expect("Failed to create signer from seed");
        let dummy_account_id = SqlAccountId(signer.account_id().clone());
        // generate nonce for the account
        let nonce = MetadataDB::generate_nonce(&command_sender, dummy_account_id.clone().0)
            .await
            .expect("Failed to generate nonce");

        let infohash_value = InfohashValue {
            infohash: InfoHash([1; 32]),
            name: "test_file.txt".to_string(),
            length: 1000,
            chunk_size: 256,
            chunk_count: 4,
            owner_account_id: dummy_account_id,
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0; 64]),
        };

        // Sign the infohash value
        let message = infohash_value.get_signature_message(&nonce);
        let signature = sign_message(&signer, &message);
        let infohash_value = InfohashValue {
            signature, // Use the signed signature
            ..infohash_value
        };

        let chunk = ChunkValue {
            chunk_hash: ChunkHash([2u8; 32]),
            k: 10,
            m: 5,
            chunk_size: 512,
            padlen: 0,
            original_chunk_size: 512,
        };

        let piece = PieceValue {
            piece_hash: PieceHash([3u8; 32]),
            piece_size: 256,
            piece_type: PieceType::Data,
            miners: vec![Compact(1), Compact(2)],
        };

        // Insert the object
        MetadataDB::insert_object(
            &command_sender,
            infohash_value,
            &nonce,
            vec![(chunk, vec![piece])],
        )
        .await
        .expect("Failed to insert object");

        // Query the piece by its hash
        let queried_piece = MetadataDB::get_piece(&command_sender, PieceHash([3u8; 32]))
            .await
            .expect("Failed to get piece");
        assert_eq!(queried_piece.piece_hash, PieceHash([3u8; 32]));
        assert_eq!(queried_piece.piece_size, 256);
        assert_eq!(queried_piece.miners, vec![Compact(1), Compact(2)]);

        // Abort the background task
        handle.abort();
        // temp_file will be automatically cleaned up when it goes out of scope
    }

    #[tokio::test]
    async fn test_get_non_existant_piece() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let db_path = setup_test_db(&temp_file);
        let crsqlite_lib = PathBuf::from("../../crsqlite/crsqlite.so");

        let (mut db, command_sender) = MetadataDB::new(&db_path, &crsqlite_lib).unwrap();

        // Start the DB event processing in the background
        let handle = tokio::spawn(async move {
            db.process_events().await;
        });

        // Attempt to get a piece that does not exist
        let result = MetadataDB::get_piece(&command_sender, PieceHash([99u8; 32])).await;

        // print result
        println!("Result: {:?}", result);

        assert!(result.is_err());
        // TODO: check if result is metadatadb error rusqlite::Error::QueryReturnedNoRows
        assert!(matches!(
            result,
            Err(MetadataDBError::Database(
                rusqlite::Error::ExecuteReturnedResults
            ))
        ));

        // Abort the background task
        handle.abort();
        // temp_file will be automatically cleaned up when it goes out of scope
    }

    #[tokio::test]
    async fn test_insert_and_query_piece_repair_history() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let db_path = setup_test_db(&temp_file);
        let crsqlite_lib = PathBuf::from("../../crsqlite/crsqlite.so");

        let (mut db, command_sender) = MetadataDB::new(&db_path, &crsqlite_lib).unwrap();

        // Start the DB event processing in the background
        let handle = tokio::spawn(async move {
            db.process_events().await;
        });

        // Create dummy account
        let decoded =
            hex::decode("ba4f8ae740dd5f7950081049e32f6ed071e3dd3d8d6f3c5073caf22924569062")
                .unwrap();
        // Create dummy account
        let test_phrase: &[u8] = decoded.as_slice();
        let signer = signer_from_seed(test_phrase).expect("Failed to create signer from seed");
        let dummy_account_id = SqlAccountId(signer.account_id().clone());
        // generate nonce for the account
        let nonce = MetadataDB::generate_nonce(&command_sender, dummy_account_id.clone().0)
            .await
            .expect("Failed to generate nonce");

        let infohash_value = InfohashValue {
            infohash: InfoHash([1; 32]),
            name: "test_file.txt".to_string(),
            length: 1000,
            chunk_size: 256,
            chunk_count: 4,
            owner_account_id: dummy_account_id,
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0; 64]),
        };

        // Sign the infohash value
        let message = infohash_value.get_signature_message(&nonce);
        let signature = sign_message(&signer, &message);
        let infohash_value = InfohashValue {
            signature, // Use the signed signature
            ..infohash_value
        };

        let chunk = ChunkValue {
            chunk_hash: ChunkHash([2u8; 32]),
            k: 10,
            m: 5,
            chunk_size: 512,
            padlen: 0,
            original_chunk_size: 512,
        };

        let piece = PieceValue {
            piece_hash: PieceHash([3u8; 32]),
            piece_size: 256,
            piece_type: PieceType::Data,
            miners: vec![Compact(1), Compact(2)],
        };

        // Insert the object first to create the chunk
        MetadataDB::insert_object(
            &command_sender,
            infohash_value.clone(),
            &nonce,
            vec![(chunk.clone(), vec![piece])],
        )
        .await
        .expect("Failed to insert object");

        // Create test data for piece repair history
        let piece_repair_hash = [4u8; 32];
        let piece_repair_history = PieceChallengeHistory {
            piece_repair_hash,
            piece_hash: PieceHash([3u8; 32]),
            chunk_hash: ChunkHash([2u8; 32]), // References the chunk we just inserted
            validator_id: Compact(1),
            timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0; 64]),
        };

        // Insert the piece repair history
        MetadataDB::insert_piece_repair_history(&command_sender, piece_repair_history.clone())
            .await
            .expect("Failed to insert piece repair history");

        // Query the piece repair history
        let queried_history =
            MetadataDB::get_piece_repair_history(&command_sender, piece_repair_hash)
                .await
                .expect("Failed to get piece repair history");

        assert_eq!(queried_history.piece_repair_hash, piece_repair_hash);
        assert_eq!(queried_history.piece_hash, PieceHash([3u8; 32]));
        assert_eq!(queried_history.chunk_hash, ChunkHash([2u8; 32]));
        assert_eq!(queried_history.validator_id, Compact(1));

        // Abort the background task
        handle.abort();
        // temp_file will be automatically cleaned up when it goes out of scope
    }

    #[tokio::test]
    async fn test_insert_and_query_chunk_challenge_history() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let db_path = setup_test_db(&temp_file);
        let crsqlite_lib = PathBuf::from("../../crsqlite/crsqlite.so");

        let (mut db, command_sender) = MetadataDB::new(&db_path, &crsqlite_lib).unwrap();

        // Start the DB event processing in the background
        let handle = tokio::spawn(async move {
            db.process_events().await;
        });

        // Create dummy account
        let decoded =
            hex::decode("ba4f8ae740dd5f7950081049e32f6ed071e3dd3d8d6f3c5073caf22924569062")
                .unwrap();
        // Create dummy account
        let test_phrase: &[u8] = decoded.as_slice();
        let signer = signer_from_seed(test_phrase).expect("Failed to create signer from seed");
        let dummy_account_id = SqlAccountId(signer.account_id().clone());
        // generate nonce for the account
        let nonce = MetadataDB::generate_nonce(&command_sender, dummy_account_id.clone().0)
            .await
            .expect("Failed to generate nonce");

        let infohash_value = InfohashValue {
            infohash: InfoHash([1; 32]),
            name: "test_file.txt".to_string(),
            length: 1000,
            chunk_size: 256,
            chunk_count: 4,
            owner_account_id: dummy_account_id,
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0; 64]),
        };

        // Sign the infohash value
        let message = infohash_value.get_signature_message(&nonce);
        let signature = sign_message(&signer, &message);
        let infohash_value = InfohashValue {
            signature, // Use the signed signature
            ..infohash_value
        };

        let chunk = ChunkValue {
            chunk_hash: ChunkHash([2u8; 32]),
            k: 10,
            m: 5,
            chunk_size: 512,
            padlen: 0,
            original_chunk_size: 512,
        };

        let piece = PieceValue {
            piece_hash: PieceHash([3u8; 32]),
            piece_size: 256,
            piece_type: PieceType::Data,
            miners: vec![Compact(1), Compact(2)],
        };

        // Insert the object first to create the chunk
        MetadataDB::insert_object(
            &command_sender,
            infohash_value.clone(),
            &nonce,
            vec![(chunk, vec![piece])],
        )
        .await
        .expect("Failed to insert object");

        // First insert a piece repair history that the chunk challenge history will reference
        let piece_repair_hash = [4u8; 32];
        let piece_repair_history = PieceChallengeHistory {
            piece_repair_hash,
            piece_hash: PieceHash([3u8; 32]),
            chunk_hash: ChunkHash([2u8; 32]), // References the chunk we just inserted
            validator_id: Compact(1),
            timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0; 64]),
        };

        // Insert the piece repair history first
        MetadataDB::insert_piece_repair_history(&command_sender, piece_repair_history)
            .await
            .expect("Failed to insert piece repair history");

        // Create test data for chunk challenge history
        let challenge_hash = [5u8; 32];
        let chunk_challenge_history = ChunkChallengeHistory {
            challenge_hash,
            chunk_hash: ChunkHash([2u8; 32]), // References the chunk we just inserted
            validator_id: Compact(1),
            miners_challenged: vec![Compact(1), Compact(2)],
            miners_successful: vec![Compact(1)],
            piece_repair_hash, // References the piece repair history we just inserted
            timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0; 64]),
        };

        // Insert the chunk challenge history
        MetadataDB::insert_chunk_challenge_history(
            &command_sender,
            chunk_challenge_history.clone(),
        )
        .await
        .expect("Failed to insert chunk challenge history");

        // Query the chunk challenge history
        let queried_history =
            MetadataDB::get_chunk_challenge_history(&command_sender, challenge_hash)
                .await
                .expect("Failed to get chunk challenge history");

        assert_eq!(queried_history.challenge_hash, challenge_hash);
        assert_eq!(queried_history.chunk_hash, ChunkHash([2u8; 32]));
        assert_eq!(queried_history.validator_id, Compact(1));
        assert_eq!(
            queried_history.miners_challenged,
            vec![Compact(1), Compact(2)]
        );
        assert_eq!(queried_history.miners_successful, vec![Compact(1)]);
        assert_eq!(queried_history.piece_repair_hash, piece_repair_hash);

        // Abort the background task
        handle.abort();
        // temp_file will be automatically cleaned up when it goes out of scope
    }

    #[tokio::test]
    async fn test_insert_object_updates_piece_miners() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let db_path = setup_test_db(&temp_file);
        let crsqlite_lib = PathBuf::from("../../crsqlite/crsqlite.so");

        let (mut db, command_sender) = MetadataDB::new(&db_path, &crsqlite_lib).unwrap();

        // Start the DB event processing in the background
        let handle = tokio::spawn(async move {
            db.process_events().await;
        });

        // Create first object with a piece that has miners [1, 2]
        let infohash1 = InfoHash([1u8; 32]);
        // Create dummy account
        let decoded =
            hex::decode("ba4f8ae740dd5f7950081049e32f6ed071e3dd3d8d6f3c5073caf22924569062")
                .unwrap();
        // Create dummy account
        let test_phrase: &[u8] = decoded.as_slice();
        let signer = signer_from_seed(test_phrase).expect("Failed to create signer from seed");
        let dummy_account_id = SqlAccountId(signer.account_id().clone());
        // generate nonce for the account
        let nonce = MetadataDB::generate_nonce(&command_sender, dummy_account_id.clone().0)
            .await
            .expect("Failed to generate nonce");

        let infohash_value1 = InfohashValue {
            infohash: infohash1,
            name: "test_object_1".to_string(),
            length: 1000,
            chunk_size: 256,
            chunk_count: 1,
            owner_account_id: dummy_account_id.clone(),
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0; 64]),
        };

        // Sign the infohash value
        let message = infohash_value1.get_signature_message(&nonce);
        let signature = sign_message(&signer, &message);
        let infohash_value1 = InfohashValue {
            signature, // Use the signed signature
            ..infohash_value1
        };

        let chunk1 = ChunkValue {
            chunk_hash: ChunkHash([2u8; 32]),
            k: 10,
            m: 5,
            chunk_size: 512,
            padlen: 0,
            original_chunk_size: 512,
        };

        let piece1 = PieceValue {
            piece_hash: PieceHash([3u8; 32]), // Same piece hash for both objects
            piece_size: 256,
            piece_type: PieceType::Data,
            miners: vec![Compact(1), Compact(2)], // Initial miners
        };

        // Insert the first object
        MetadataDB::insert_object(
            &command_sender,
            infohash_value1,
            &nonce,
            vec![(chunk1, vec![piece1])],
        )
        .await
        .expect("Failed to insert first object");

        // Verify initial miners
        let initial_pieces = MetadataDB::get_pieces_by_chunk(&command_sender, ChunkHash([2u8; 32]))
            .await
            .expect("Failed to get pieces by chunk");
        assert_eq!(initial_pieces.len(), 1);
        assert_eq!(initial_pieces[0].miners, vec![Compact(1), Compact(2)]);

        // Create second object with the SAME piece but different miners [2, 3, 4]
        let infohash2 = InfoHash([4u8; 32]);
        let infohash_value2 = InfohashValue {
            infohash: infohash2,
            name: "test_object_2".to_string(),
            length: 2000,
            chunk_size: 256,
            chunk_count: 1,
            owner_account_id: dummy_account_id.clone(),
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([1; 64]),
        };

        // create another nonce for the account
        let nonce = MetadataDB::generate_nonce(&command_sender, dummy_account_id.clone().0)
            .await
            .expect("Failed to generate nonce");

        // Sign the infohash value
        let message = infohash_value2.get_signature_message(&nonce);
        let signature = sign_message(&signer, &message);
        let infohash_value2 = InfohashValue {
            signature, // Use the signed signature
            ..infohash_value2
        };

        let chunk2 = ChunkValue {
            chunk_hash: ChunkHash([5u8; 32]), // Different chunk
            k: 10,
            m: 5,
            chunk_size: 512,
            padlen: 0,
            original_chunk_size: 512,
        };

        let piece2 = PieceValue {
            piece_hash: PieceHash([3u8; 32]), // SAME piece hash as piece1
            piece_size: 256,
            piece_type: PieceType::Data,
            miners: vec![Compact(2), Compact(3), Compact(4)], // Some overlap (2) and new miners (3, 4)
        };

        // Insert the second object - this should update the miners for the existing piece
        MetadataDB::insert_object(
            &command_sender,
            infohash_value2,
            &nonce,
            vec![(chunk2, vec![piece2])],
        )
        .await
        .expect("Failed to insert second object");

        // Query the piece from both chunks to verify miners were updated
        let pieces_chunk1 = MetadataDB::get_pieces_by_chunk(&command_sender, ChunkHash([2u8; 32]))
            .await
            .expect("Failed to get pieces from first chunk");

        let pieces_chunk2 = MetadataDB::get_pieces_by_chunk(&command_sender, ChunkHash([5u8; 32]))
            .await
            .expect("Failed to get pieces from second chunk");

        // Both should have the same piece with merged miners
        assert_eq!(pieces_chunk1.len(), 1);
        assert_eq!(pieces_chunk2.len(), 1);

        // The piece should now have miners [1, 2, 3, 4] (merged and deduplicated)
        let expected_miners = vec![Compact(1), Compact(2), Compact(3), Compact(4)];

        // Sort both vectors for comparison since order might vary
        let mut actual_miners_chunk1 = pieces_chunk1[0].miners.clone();
        let mut actual_miners_chunk2 = pieces_chunk2[0].miners.clone();
        actual_miners_chunk1.sort_by_key(|c| c.0);
        actual_miners_chunk2.sort_by_key(|c| c.0);

        let mut expected_sorted = expected_miners.clone();
        expected_sorted.sort_by_key(|c| c.0);

        assert_eq!(actual_miners_chunk1, expected_sorted);
        assert_eq!(actual_miners_chunk2, expected_sorted);

        // Verify that both pieces have the same miners (since they're the same piece)
        assert_eq!(pieces_chunk1[0].miners, pieces_chunk2[0].miners);

        // generate another nonce for the account
        let nonce = MetadataDB::generate_nonce(&command_sender, dummy_account_id.clone().0)
            .await
            .expect("Failed to generate nonce");

        // Test edge case: inserting the same piece again with duplicate miners should not change anything
        let infohash3 = InfoHash([6u8; 32]);
        let infohash_value3 = InfohashValue {
            infohash: infohash3,
            name: "test_object_3".to_string(),
            length: 3000,
            chunk_size: 256,
            chunk_count: 1,
            owner_account_id: dummy_account_id.clone(),
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([2u8; 64]),
        };

        // Sign the infohash value
        let message = infohash_value3.get_signature_message(&nonce);
        let signature = sign_message(&signer, &message);
        let infohash_value3 = InfohashValue {
            signature, // Use the signed signature
            ..infohash_value3
        };

        let chunk3 = ChunkValue {
            chunk_hash: ChunkHash([7u8; 32]), // Different chunk
            k: 10,
            m: 5,
            chunk_size: 512,
            padlen: 0,
            original_chunk_size: 512,
        };

        let piece3 = PieceValue {
            piece_hash: PieceHash([3u8; 32]), // SAME piece hash as piece1
            piece_size: 256,
            piece_type: PieceType::Data,
            miners: vec![Compact(2), Compact(3)], // Miners that already exist
        };

        // Insert the third object - should not change miners since they already exist
        MetadataDB::insert_object(
            &command_sender,
            infohash_value3,
            &nonce,
            vec![(chunk3, vec![piece3])],
        )
        .await
        .expect("Failed to insert third object");

        // Query the piece from the third chunk to verify miners were not changed
        let pieces_chunk3 = MetadataDB::get_pieces_by_chunk(&command_sender, ChunkHash([7u8; 32]))
            .await
            .expect("Failed to get pieces from third chunk");

        let mut actual_miners_chunk3 = pieces_chunk3[0].miners.clone();
        actual_miners_chunk3.sort_by_key(|c| c.0);

        assert_eq!(actual_miners_chunk3, expected_sorted);

        // Abort the background task
        handle.abort();
        // temp_file will be automatically cleaned up when it goes out of scope
    }

    #[tokio::test]
    async fn test_delete_infohash() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let db_path = setup_test_db(&temp_file);
        let crsqlite_lib = PathBuf::from("../../crsqlite/crsqlite.so");

        let (mut db, command_sender) = MetadataDB::new(&db_path, &crsqlite_lib).unwrap();

        // Start the DB event processing in the background
        let handle = tokio::spawn(async move {
            db.process_events().await;
        });

        // Create dummy account
        let decoded =
            hex::decode("ba4f8ae740dd5f7950081049e32f6ed071e3dd3d8d6f3c5073caf22924569062")
                .unwrap();
        // Create dummy account
        let test_phrase: &[u8] = decoded.as_slice();
        let signer = signer_from_seed(test_phrase).expect("Failed to create signer from seed");
        let dummy_account_id = SqlAccountId(signer.account_id().clone());

        // generate nonce for the account
        let nonce = MetadataDB::generate_nonce(&command_sender, dummy_account_id.clone().0)
            .await
            .expect("Failed to generate nonce");

        let infohash_value = InfohashValue {
            infohash: InfoHash([1; 32]),
            name: "test_file.txt".to_string(),
            length: 1000,
            chunk_size: 256,
            chunk_count: 4,
            owner_account_id: dummy_account_id.clone(),
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0; 64]),
        };

        // Sign the infohash value
        let message = infohash_value.get_signature_message(&nonce);
        let signature = sign_message(&signer, &message);
        let infohash_value = InfohashValue {
            signature, // Use the signed signature
            ..infohash_value
        };
        let chunk = ChunkValue {
            chunk_hash: ChunkHash([2u8; 32]),
            k: 10,
            m: 5,
            chunk_size: 512,
            padlen: 0,
            original_chunk_size: 512,
        };
        let piece = PieceValue {
            piece_hash: PieceHash([3u8; 32]),
            piece_size: 256,
            piece_type: PieceType::Data,
            miners: vec![Compact(1), Compact(2)],
        };
        // Insert the object
        MetadataDB::insert_object(
            &command_sender,
            infohash_value.clone(),
            &nonce,
            vec![(chunk.clone(), vec![piece])],
        )
        .await
        .expect("Failed to insert object");

        // Query the infohash to ensure it exists
        let queried_infohash = MetadataDB::get_infohash(&command_sender, infohash_value.infohash)
            .await
            .expect("Failed to get infohash");
        assert_eq!(queried_infohash.infohash, infohash_value.infohash);
        assert_eq!(queried_infohash.name, infohash_value.name);
        assert_eq!(queried_infohash.length, infohash_value.length);
        assert_eq!(queried_infohash.chunk_size, infohash_value.chunk_size);
        assert_eq!(queried_infohash.chunk_count, infohash_value.chunk_count);
        assert_eq!(
            queried_infohash.owner_account_id,
            infohash_value.owner_account_id
        );

        // Generate another nonce so we can delete the infohash
        let nonce = MetadataDB::generate_nonce(&command_sender, dummy_account_id.clone().0)
            .await
            .expect("Failed to generate nonce");

        // Sign the infohash value again with the new nonce
        let message = infohash_value.get_signature_message(&nonce);
        let signature = sign_message(&signer, &message);
        let infohash_value = InfohashValue {
            signature, // Use the signed signature
            ..infohash_value.clone()
        };

        // Delete the infohash
        MetadataDB::delete_infohash(&command_sender, infohash_value.clone(), &nonce)
            .await
            .expect("Failed to delete infohash");

        // Attempt to query the infohash again, expecting an error
        let result =
            MetadataDB::get_infohash(&command_sender, infohash_value.clone().infohash).await;
        assert!(result.is_err());

        // Verify that the chunks and pieces associated with the infohash are also deleted
        let queried_chunks = MetadataDB::get_chunks_by_infohash(
            &command_sender,
            infohash_value.clone().infohash.to_vec(),
        )
        .await
        .expect("Failed to get chunks by infohash");
        assert!(
            queried_chunks.is_empty(),
            "Chunks should be empty after deletion"
        );
        let queried_pieces =
            MetadataDB::get_pieces_by_chunk(&command_sender, chunk.clone().chunk_hash)
                .await
                .expect("Failed to get pieces by chunk");
        assert!(
            queried_pieces.is_empty(),
            "Pieces should be empty after deletion"
        );

        handle.abort();
    }
}
