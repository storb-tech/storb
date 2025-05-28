use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::http::response;
use crabtensor::sign::KeypairSignature;
use r2d2_sqlite::{SqliteConnectionManager, rusqlite::params};
use r2d2::Pool;
use rusqlite::{Connection, OptionalExtension, Result};
use subxt::ext::codec::Compact;
use tokio::sync::mpsc;
use tracing::{debug, error};

use super::models::{
    ChunkChallengeHistory, ChunkValue, InfohashValue, PieceChallengeHistory, SqlDateTime,
};
use crate::{constants::DB_MPSC_BUFFER_SIZE, swarm::models::PieceValue};

#[derive(Debug)]
pub enum MetadataDBError {
    Database(rusqlite::Error),
    Pool(r2d2::Error),
    InvalidPath(String),
    MissingTable(String),
    ExtensionLoad(String),
}

impl std::fmt::Display for MetadataDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetadataDBError::Database(e) => write!(f, "Database error: {}", e),
            MetadataDBError::Pool(e) => write!(f, "Pool error: {}", e),
            MetadataDBError::InvalidPath(path) => write!(f, "Invalid path: {}", path),
            MetadataDBError::MissingTable(table) => write!(f, "Missing table: {}", table),
            MetadataDBError::ExtensionLoad(msg) => write!(f, "Extension load error: {}", msg),
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
    InsertObject {
        infohash_value: InfohashValue,
        chunks_with_pieces: Vec<(ChunkValue, Vec<PieceValue>)>,
        response_sender: mpsc::Sender<Result<(), MetadataDBError>>,
    },
    GetRandomChunk {
        response_sender: mpsc::Sender<Result<ChunkValue, MetadataDBError>>,
    },
    InsertPieceRepairHistory {
        piece_repair_history: PieceChallengeHistory,
        response_sender: mpsc::Sender<Result<(), MetadataDBError>>,
    },
    InsertChunkChallengeHistory {
        chunk_challenge_history: ChunkChallengeHistory,
        response_sender: mpsc::Sender<Result<(), MetadataDBError>>,
    },
    GetPiecesByChunk {
        chunk_hash: [u8; 32],
        response_sender: mpsc::Sender<Result<Vec<PieceValue>, MetadataDBError>>,
    },
    GetChunksByInfohash {
        infohash: Vec<u8>,
        response_sender: mpsc::Sender<Result<Vec<ChunkValue>, MetadataDBError>>,
    },
    GetInfohash {
        infohash: [u8; 32],
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
                "CR-Sqlite extension library path does not exist: {}",
                crsqlite_lib.display()
            )));
        }

        // Create connection manager
        let crsqlite_lib = crsqlite_lib.to_path_buf();
        let manager = SqliteConnectionManager::file(db_path)
            .with_init(move |conn| {
                Self::load_crsqlite_extension(conn, &crsqlite_lib)
                    .map_err(|e| match e {
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
            Self::validate_database(&*conn)?;
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

    pub async fn handle_insert_piece_repair_history(
        &self,
        value: &PieceChallengeHistory,
        response_sender: mpsc::Sender<Result<(), MetadataDBError>>,
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);
        let value = value.clone();
        
        let result = tokio::task::spawn_blocking(move || {
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
        if response_sender.send(Ok(result)).await.is_err() {
            error!("Failed to send piece repair history response");
        }

        Ok(())
    }

    pub fn insert_piece_repair_history(
        command_sender: &mpsc::Sender<MetadataDBCommand>,
        piece_repair_history: PieceChallengeHistory,
    ) -> Result<(), MetadataDBError> {
        // Create a channel for the response
        let (response_sender, mut response_receiver) =
            mpsc::channel::<Result<(), MetadataDBError>>(1);

        // Send the command to insert piece repair history
        command_sender
            .blocking_send(MetadataDBCommand::InsertPieceRepairHistory {
                piece_repair_history,
                response_sender,
            })
            .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))?;

        // Wait for the response
        match response_receiver.blocking_recv() {
            Some(result) => result,
            None => Err(MetadataDBError::Database(
                rusqlite::Error::ExecuteReturnedResults,
            )),
        }
    }

    pub async fn handle_insert_chunk_challenge_history(
        &self,
        value: &ChunkChallengeHistory,
        response_sender: mpsc::Sender<Result<(), MetadataDBError>>
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);
        let value = value.clone();
        
        let result = tokio::task::spawn_blocking(move || {
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
        if response_sender.send(Ok(result)).await.is_err() {
            error!("Failed to send chunk challenge history response");
        }

        Ok(())
    }

    pub fn insert_chunk_challenge_history(
        command_sender: &mpsc::Sender<MetadataDBCommand>,
        chunk_challenge_history: ChunkChallengeHistory,
    ) -> Result<(), MetadataDBError> {
        // Create a channel for the response
        let (response_sender, mut response_receiver) =
            mpsc::channel::<Result<(), MetadataDBError>>(1);

        // Send the command to insert chunk challenge history
        command_sender
            .blocking_send(MetadataDBCommand::InsertChunkChallengeHistory {
                chunk_challenge_history,
                response_sender,
            })
            .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))?;

        // Wait for the response
        match response_receiver.blocking_recv() {
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
        
        let result = tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let tx = conn.unchecked_transaction()?;

            // Insert infohash
            tx.execute(
                "INSERT INTO infohashes (infohash, length, chunk_size, chunk_count, creation_timestamp, signature) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    infohash_value.infohash,
                    infohash_value.length,
                    infohash_value.chunk_size,
                    infohash_value.chunk_count,
                    SqlDateTime(infohash_value.creation_timestamp),
                    infohash_value.signature.0.to_vec()
                ],
            )?;

            // Prepare statements for better performance with multiple inserts
            let mut chunk_stmt = tx.prepare(
                "INSERT INTO chunks (chunk_hash, chunk_idx, k, m, chunk_size, padlen, original_chunk_size) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"
            )?;

            let mut tracker_chunk_stmt = tx.prepare(
                "INSERT INTO tracker_chunks (infohash, chunk_idx, chunk_hash) VALUES (?1, ?2, ?3)",
            )?;

            let mut piece_stmt = tx.prepare(
                "INSERT INTO pieces (piece_hash, validator_id, chunk_idx, piece_idx, piece_size, piece_type, miners) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"
            )?;

            let mut chunk_pieces_stmt = tx.prepare(
                "INSERT INTO chunk_pieces (chunk_hash, piece_idx, piece_hash) VALUES (?1, ?2, ?3)",
            )?;

            // Insert chunks and their pieces
            for (chunk, pieces) in chunks_with_pieces {
                // Insert chunk
                chunk_stmt.execute(params![
                    chunk.chunk_hash,
                    chunk.chunk_idx,
                    chunk.k,
                    chunk.m,
                    chunk.chunk_size,
                    chunk.padlen,
                    chunk.original_chunk_size
                ])?;

                // Insert chunk-infohash mapping
                tracker_chunk_stmt.execute(params![
                    infohash_value.infohash,
                    chunk.chunk_idx,
                    chunk.chunk_hash
                ])?;

                // Insert all pieces for this chunk
                for piece in pieces {
                    // Convert miners to JSON string
                    let miners = serde_json::to_string(&piece.miners).map_err(|e| {
                        MetadataDBError::Database(rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        ))
                    })?;
                    // Insert piece
                    piece_stmt.execute(params![
                        piece.piece_hash,
                        piece.validator_id.0,
                        piece.chunk_idx,
                        piece.piece_idx,
                        piece.piece_size,
                        piece.piece_type as u8,
                        miners
                    ])?;

                    // Insert chunk-piece mapping
                    chunk_pieces_stmt.execute(params![
                        chunk.chunk_hash,
                        piece.piece_idx,
                        piece.piece_hash
                    ])?;
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
        if let Err(e) = response_sender.send(Ok(result)).await {
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
        chunks_with_pieces: Vec<(ChunkValue, Vec<PieceValue>)>,
    ) -> Result<(), MetadataDBError> {
        // Create a channel for the response
        let (response_sender, mut response_receiver) =
            mpsc::channel::<Result<(), MetadataDBError>>(1);

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

    pub async fn handle_get_pieces_by_chunk(
        &self,
        chunk_hash: &[u8; 32],
        response_sender: mpsc::Sender<Result<Vec<PieceValue>, MetadataDBError>>,
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);
        let chunk_hash = *chunk_hash;
        
        let result = tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let mut stmt = conn.prepare(
                "SELECT p.piece_hash, p.validator_id, p.chunk_idx, p.piece_idx, p.piece_size, p.piece_type, p.miners 
                    FROM pieces p 
                    INNER JOIN chunk_pieces cp ON p.piece_hash = cp.piece_hash 
                    WHERE cp.chunk_hash = ?1
                    ORDER BY p.piece_idx",
            )?;

            let pieces = stmt
                .query_map(params![chunk_hash], |row| {
                    Ok(PieceValue {
                        piece_hash: row.get(0)?,
                        validator_id: Compact(row.get::<_, u16>(1)?),
                        chunk_idx: row.get(2)?,
                        piece_idx: row.get(3)?,
                        piece_size: row.get(4)?,
                        piece_type: row.get::<_, u8>(5)?.try_into().map_err(|_| {
                            rusqlite::Error::FromSqlConversionFailure(
                                5,
                                rusqlite::types::Type::Integer,
                                Box::new(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "Invalid piece type",
                                )),
                            )
                        })?,
                        miners: serde_json::from_str(&row.get::<_, String>(6)?.as_str()).map_err(
                            |e| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    6,
                                    rusqlite::types::Type::Text,
                                    Box::new(e),
                                )
                            },
                        )?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?;

            Ok::<Vec<PieceValue>, MetadataDBError>(pieces)
        }).await.map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        // Send the result through the response channel
        if response_sender.send(Ok(result)).await.is_err() {
            error!("Failed to send pieces response");
        }

        Ok(())
    }

    pub async fn get_pieces_by_chunk(
        command_sender: &mpsc::Sender<MetadataDBCommand>,
        chunk_hash: [u8; 32],
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
                "SELECT tc.chunk_hash, tc.chunk_idx, c.k, c.m, c.chunk_size, c.padlen, c.original_chunk_size 
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
                        chunk_idx: row.get(1)?,
                        k: row.get(2)?,
                        m: row.get(3)?,
                        chunk_size: row.get(4)?,
                        padlen: row.get(5)?,
                        original_chunk_size: row.get(6)?,
                    })
                })?
                .collect();

            chunks.map_err(MetadataDBError::from)
        }).await.map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

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
        infohash: &[u8; 32],
        response_sender: mpsc::Sender<Result<InfohashValue, MetadataDBError>>,
    ) -> Result<(), MetadataDBError> {
        let pool = Arc::clone(&self.pool);
        let infohash = *infohash;
        
        let result = tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let mut stmt = conn.prepare(
                "SELECT infohash, length, chunk_size, chunk_count, creation_timestamp, signature 
                 FROM infohashes 
                 WHERE infohash = ?1",
            )?;

            let infohash_value = stmt.query_row(params![infohash], |row| {
                Ok(InfohashValue {
                    infohash: row.get(0)?,
                    length: row.get(1)?,
                    chunk_size: row.get(2)?,
                    chunk_count: row.get(3)?,
                    creation_timestamp: row.get::<_, SqlDateTime>(4)?.0,
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

            Ok::<InfohashValue, MetadataDBError>(infohash_value)
        }).await.map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        // Send result through response channel
        if response_sender.send(Ok(result)).await.is_err() {
            error!("Failed to send infohash response");
        }

        Ok(())
    }

    pub async fn get_infohash(
        command_sender: &mpsc::Sender<MetadataDBCommand>,
        infohash: [u8; 32],
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
                "SELECT chunk_hash, chunk_idx, k, m, chunk_size, padlen, original_chunk_size 
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
                    chunk_idx: row.get(1)?,
                    k: row.get(2)?,
                    m: row.get(3)?,
                    chunk_size: row.get(4)?,
                    padlen: row.get(5)?,
                    original_chunk_size: row.get(6)?,
                })
            })?;

            Ok::<ChunkValue, MetadataDBError>(chunk)
        }).await.map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

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
        }).await.map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        // Send result through response channel
        if response_sender.send(Ok(result)).await.is_err() {
            error!("Failed to send piece repair history response");
        }

        Ok(())
    }

    pub fn get_piece_repair_history(
        command_sender: &mpsc::Sender<MetadataDBCommand>,
        piece_repair_hash: [u8; 32],
    ) -> Result<PieceChallengeHistory, MetadataDBError> {
        // Create a channel for the response
        let (response_sender, mut response_receiver) =
            mpsc::channel::<Result<PieceChallengeHistory, MetadataDBError>>(1);

        // Send the command to get piece repair history
        command_sender
            .blocking_send(MetadataDBCommand::GetPieceRepairHistory {
                piece_repair_hash,
                response_sender,
            })
            .map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))?;

        // Wait for the response
        match response_receiver.blocking_recv() {
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
        }).await.map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        // Send result through response channel
        if response_sender.send(Ok(result)).await.is_err() {
            error!("Failed to send chunk challenge history response");
        }

        Ok(())
    }

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

    /// Processes db events and commands.
    ///
    /// This asynchronous function continuously processes db events and commands,
    /// handling inserts, updates, and queries as they come in.
    pub async fn process_events(&mut self) {
        while let Some(command) = self.command_receiver.recv().await {
            match command {
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
                    if let Err(e) = self.handle_insert_object(
                        &infohash_value,
                        chunks_with_pieces,
                        response_sender,
                    ).await {
                        error!("Error inserting object: {:?}", e);
                    }
                }
                MetadataDBCommand::GetRandomChunk { response_sender } => {
                    debug!("Handling request for random chunk");
                    if let Err(e) = self.handle_get_random_chunk(response_sender).await {
                        error!("Error retrieving random chunk: {:?}", e);
                    }
                }
                MetadataDBCommand::InsertPieceRepairHistory {
                    piece_repair_history,
                    response_sender,
                } => {
                    debug!("Handling insert piece repair history");
                    if let Err(e) = self.handle_insert_piece_repair_history(
                        &piece_repair_history,
                        response_sender,
                    ).await {
                        error!("Error inserting piece repair history: {:?}", e);
                    }
                }
                MetadataDBCommand::InsertChunkChallengeHistory {
                    chunk_challenge_history,
                    response_sender,
                } => {
                    debug!("Handling insert chunk challenge history");
                    if let Err(e) = self.handle_insert_chunk_challenge_history(
                        &chunk_challenge_history,
                        response_sender,
                    ).await {
                        error!("Error inserting chunk challenge history: {:?}", e);
                    }
                }
                MetadataDBCommand::GetPiecesByChunk {
                    chunk_hash,
                    response_sender,
                } => {
                    debug!("Handling get pieces by chunk");
                    if let Err(e) = self.handle_get_pieces_by_chunk(&chunk_hash, response_sender).await {
                        error!("Error getting pieces by chunk: {:?}", e);
                    }
                }
                MetadataDBCommand::GetChunksByInfohash {
                    infohash,
                    response_sender,
                } => {
                    debug!("Handling get chunks by infohash");
                    if let Err(e) = self.handle_get_chunks_by_infohash(&infohash, response_sender).await {
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
                    if let Err(e) = self.handle_get_piece_repair_history(&piece_repair_hash, response_sender).await {
                        error!("Error getting piece repair history: {:?}", e);
                    }
                }
                MetadataDBCommand::GetChunkChallengeHistory {
                    challenge_hash,
                    response_sender,
                } => {
                    debug!("Handling get chunk challenge history");
                    if let Err(e) = self.handle_get_chunk_challenge_history(&challenge_hash, response_sender).await {
                        error!("Error getting chunk challenge history: {:?}", e);
                    }
                }
            }
        }
    }

}

// tests for insertion and query functions in the metadatadb
#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::path::PathBuf;
    use std::{fs, sync::Arc};

    use chrono::Utc;
    use subxt::ext::codec::Compact;
    use tokio::sync::Mutex;

    use super::*;
    use crate::{
        piece::PieceType,
        swarm::models::{ChunkValue, InfohashValue, PieceValue},
    };

    // Helper function to create a test database with schema
    fn setup_test_db() -> (PathBuf, PathBuf) {
        // Create a unique filename for each test based on timestamp
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos();

        // Create test directory if it doesn't exist
        let test_dir = PathBuf::from("test_dir_metadatadb/");
        fs::create_dir_all(&test_dir).expect("Failed to create test directory");

        // Create unique test database path
        let db_path = test_dir.join(format!("test_db_{}.sqlite", timestamp));

        // Create empty file
        fs::write(&db_path, b"").expect("Failed to write to test file");

        // Path to the CRSQLite extension
        let crsqlite_lib = PathBuf::from("/root/crsqlite/crsqlite.so");

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

        // Create tables
        conn.execute(
            "CREATE TABLE infohashes (
                infohash BLOB PRIMARY KEY,
                length INTEGER NOT NULL,
                chunk_size INTEGER NOT NULL,
                chunk_count INTEGER NOT NULL,
                creation_timestamp DATETIME NOT NULL,  /* Change TEXT to DATETIME */
                signature BLOB NOT NULL
            )",
            [],
        )
        .expect("Failed to create infohashes table");

        conn.execute(
            "CREATE TABLE chunks (
                chunk_hash BLOB PRIMARY KEY,
                chunk_idx INTEGER NOT NULL,
                k INTEGER NOT NULL,
                m INTEGER NOT NULL,
                chunk_size INTEGER NOT NULL,
                padlen INTEGER NOT NULL,
                original_chunk_size INTEGER NOT NULL
            )",
            [],
        )
        .expect("Failed to create chunks table");

        conn.execute(
            "CREATE TABLE tracker_chunks (
                infohash BLOB NOT NULL REFERENCES infohashes(infohash),
                chunk_idx INTEGER NOT NULL,
                chunk_hash BLOB NOT NULL REFERENCES chunks(chunk_hash),
                PRIMARY KEY (infohash, chunk_idx)
            )",
            [],
        )
        .expect("Failed to create tracker_chunks table");

        conn.execute(
            "CREATE TABLE pieces (
                piece_hash BLOB PRIMARY KEY,
                validator_id INTEGER NOT NULL,
                chunk_idx INTEGER NOT NULL,
                piece_idx INTEGER NOT NULL,
                piece_size INTEGER NOT NULL,
                piece_type INTEGER NOT NULL,
                miners TEXT NOT NULL
            )",
            [],
        )
        .expect("Failed to create pieces table");

        conn.execute(
            "CREATE TABLE chunk_pieces (
                chunk_hash BLOB NOT NULL REFERENCES chunks(chunk_hash),
                piece_idx INTEGER NOT NULL,
                piece_hash BLOB NOT NULL REFERENCES pieces(piece_hash),
                PRIMARY KEY (chunk_hash, piece_idx)
            )",
            [],
        )
        .expect("Failed to create chunk_pieces table");

        conn.execute(
            "CREATE TABLE piece_repair_history (
                piece_repair_hash BLOB PRIMARY KEY,
                piece_hash BLOB NOT NULL,
                chunk_hash BLOB NOT NULL REFERENCES chunks(chunk_hash),
                validator_id INTEGER NOT NULL,
                timestamp DATETIME NOT NULL,
                signature BLOB NOT NULL
            )",
            [],
        )
        .expect("Failed to create piece_repair_history table");

        conn.execute(
            "CREATE TABLE chunk_challenge_history (
                challenge_hash BLOB PRIMARY KEY,
                chunk_hash BLOB NOT NULL REFERENCES chunks(chunk_hash),
                validator_id INTEGER NOT NULL,
                miners_challenged TEXT NOT NULL,
                miners_successful TEXT NOT NULL,
                piece_repair_hash BLOB REFERENCES piece_repair_history(piece_repair_hash),
                timestamp DATETIME NOT NULL,
                signature BLOB NOT NULL
            )",
            [],
        )
        .expect("Failed to create chunk_challenge_history table");

        // Return the paths needed for tests
        (db_path, crsqlite_lib)
    }

    // Helper function to clean up test database
    fn cleanup_test_db(db_path: &PathBuf) {
        if db_path.exists() {
            // Close any open connections first (implicitly done by Rust's Drop)
            // Then remove the file
            fs::remove_file(db_path).expect("Failed to remove test database file");
        }
    }

    #[tokio::test]
    async fn test_insert_and_query_object() {
        let (db_path, crsqlite_lib) = setup_test_db();
        let (mut db, command_sender) = MetadataDB::new(&db_path, &crsqlite_lib).unwrap();

        // Start the DB event processing in the background
        let handle = tokio::spawn(async move {
            db.process_events().await;
        });

        // Create test data (unchanged)
        let infohash = [1u8; 32];
        let infohash_value = InfohashValue {
            infohash,
            length: 1000,
            chunk_size: 256,
            chunk_count: 4,
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0u8; 64]),
        };

        let chunk = ChunkValue {
            chunk_hash: [2u8; 32],
            chunk_idx: 0,
            k: 10,
            m: 5,
            chunk_size: 512,
            padlen: 0,
            original_chunk_size: 512,
        };

        let piece = PieceValue {
            piece_hash: [3u8; 32],
            validator_id: Compact(1),
            chunk_idx: 0,
            piece_idx: 0,
            piece_size: 256,
            piece_type: PieceType::Data,
            miners: vec![Compact(1), Compact(2)],
        };
        let piece_clone = piece.clone();

        // Insert the object and wait for it to complete
        MetadataDB::insert_object(
            &command_sender,
            infohash_value.clone(),
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
        let queried_chunks =
            MetadataDB::get_chunks_by_infohash(&command_sender, infohash.to_vec())
            .await
            .expect("Failed to get chunks by infohash");
        assert_eq!(queried_chunks.len(), 1);
        assert_eq!(queried_chunks[0].chunk_hash, chunk.chunk_hash);
        assert_eq!(queried_chunks[0].chunk_idx, chunk.chunk_idx);
        assert_eq!(queried_chunks[0].k, chunk.k);
        assert_eq!(queried_chunks[0].m, chunk.m);
        assert_eq!(queried_chunks[0].chunk_size, chunk.chunk_size);
        assert_eq!(queried_chunks[0].padlen, chunk.padlen);
        assert_eq!(queried_chunks[0].original_chunk_size, chunk.original_chunk_size);

        // Query the pieces by chunk
        let queried_pieces = MetadataDB::get_pieces_by_chunk(&command_sender, chunk.chunk_hash)
            .await
            .expect("Failed to get pieces by chunk");
        assert_eq!(queried_pieces.len(), 1);
        assert_eq!(queried_pieces[0].piece_hash, piece_clone.piece_hash);
        assert_eq!(queried_pieces[0].validator_id, piece_clone.validator_id);
        assert_eq!(queried_pieces[0].chunk_idx, piece_clone.chunk_idx);
        assert_eq!(queried_pieces[0].piece_idx, piece_clone.piece_idx);
        assert_eq!(queried_pieces[0].piece_size, piece_clone.piece_size);
        assert_eq!(queried_pieces[0].miners, piece_clone.miners);

        // Clean up test file
        cleanup_test_db(&db_path);
        
        // Abort the background task
        handle.abort();
    }

    #[tokio::test]
    async fn test_insert_object_and_get_random_chunk() {
        let (db_path, crsqlite_lib) = setup_test_db();
        let (mut db, command_sender) = MetadataDB::new(&db_path, &crsqlite_lib).unwrap();

        // Start the DB event processing in the background
        let handle = tokio::spawn(async move {
            db.process_events().await;
        });

        // Create test data (unchanged)
        let infohash = [1u8; 32];
        let infohash_value = InfohashValue {
            infohash,
            length: 1000,
            chunk_size: 256,
            chunk_count: 4,
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0u8; 64]),
        };

        let chunk = ChunkValue {
            chunk_hash: [2u8; 32],
            chunk_idx: 0,
            k: 10,
            m: 5,
            chunk_size: 512,
            padlen: 0,
            original_chunk_size: 512,
        };

        let piece = PieceValue {
            piece_hash: [3u8; 32],
            validator_id: Compact(1),
            chunk_idx: 0,
            piece_idx: 0,
            piece_size: 256,
            piece_type: PieceType::Data,
            miners: vec![Compact(1), Compact(2)],
        };

        // Insert the object
        MetadataDB::insert_object(
            &command_sender,
            infohash_value,
            vec![(chunk, vec![piece])],
        )
        .await
        .expect("Failed to insert object");

        // Query a random chunk
        let random_chunk = MetadataDB::get_random_chunk(&command_sender)
        .await
        .expect("Failed to get random chunk");
        assert_eq!(random_chunk.chunk_hash, [2u8; 32]);

        // Clean up test file
        cleanup_test_db(&db_path);
        
        // Abort the background task
        handle.abort();
    }
}
