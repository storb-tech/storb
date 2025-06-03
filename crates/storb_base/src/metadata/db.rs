use std::path::Path;
use std::sync::Arc;

use crabtensor::sign::KeypairSignature;
use r2d2::Pool;
use r2d2_sqlite::{rusqlite::params, SqliteConnectionManager};
use rusqlite::{Connection, Result};
use subxt::ext::codec::Compact;
use tokio::sync::mpsc;
use tracing::{debug, error};

use super::models::{
    ChunkChallengeHistory, ChunkValue, InfohashValue, PieceChallengeHistory, SqlDateTime,
};
use crate::{
    constants::DB_MPSC_BUFFER_SIZE,
    metadata::models::PieceValue,
    piece::{ChunkHash, InfoHash, PieceHash},
    NodeUID,
};

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

            // Insert infohash
            // If it already exists, it will be ignored due to the unique constraint
            match tx.execute(
                "INSERT INTO infohashes (infohash, name, length, chunk_size, chunk_count, creation_timestamp, signature) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    infohash_value.infohash,
                    infohash_value.name,
                    infohash_value.length,
                    infohash_value.chunk_size,
                    infohash_value.chunk_count,
                    SqlDateTime(infohash_value.creation_timestamp),
                    infohash_value.signature.0.to_vec()
                ],
            ) {
                Ok(_) => {}
                Err(e) if e.to_string().contains("UNIQUE constraint failed") => {
                    // Ignore if infohash already exists
                    debug!("Infohash {} already exists, skipping insert", hex::encode(infohash_value.infohash));
                }
                Err(e) => return Err(MetadataDBError::Database(e)),
            }

            // Prepare statements for better performance with multiple inserts
            let mut chunk_stmt = tx.prepare(
                "INSERT INTO chunks (chunk_hash, k, m, chunk_size, padlen, original_chunk_size) VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
            )?;

            let mut tracker_chunk_stmt = tx.prepare(
                "INSERT INTO tracker_chunks (infohash, chunk_idx, chunk_hash) VALUES (?1, ?2, ?3)",
            )?;

            let mut piece_stmt = tx.prepare(
                "INSERT INTO pieces (piece_hash, piece_size, piece_type, miners) VALUES (?1, ?2, ?3, ?4)"
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
                    // Insert piece
                    // If it already exists, update the miners
                    // make sure to check if a miner uid already exists in the miners list
                    // if it does, just dont insert that particular miner uid
                    // TODO: this is so cooked lmao
                    match piece_stmt.execute(params![
                        piece.piece_hash,
                        piece.piece_size,
                        piece.piece_type.clone() as u8,
                        miners
                    ]) {
                        Ok(_) => {}
                        Err(e) if e.to_string().contains("UNIQUE constraint failed") => {
                            // Read The miners from the database
                            // Check to see if any of the miners we want to insert are already in the database
                            let mut existing_miners_stmt = conn.prepare(
                                "SELECT miners FROM pieces WHERE piece_hash = ?1",
                            )?;
                            let existing_miners: String = existing_miners_stmt
                                .query_row(params![piece.piece_hash], |row| {
                                    row.get(0)
                                })
                                .map_err(MetadataDBError::Database)?;
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
                            // Update the piece with the new miners
                            let updated_miners = serde_json::to_string(&existing_miners).map_err(
                                |e| {
                                    MetadataDBError::Database(rusqlite::Error::FromSqlConversionFailure(
                                        0,
                                        rusqlite::types::Type::Text,
                                        Box::new(e),
                                    ))
                                },
                            )?;
                            conn.execute(
                                "UPDATE pieces SET miners = ?1 WHERE piece_hash = ?2",
                                params![updated_miners, piece.piece_hash],
                            )
                            .map_err(MetadataDBError::Database)?;
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
                "SELECT infohash, name, length, chunk_size, chunk_count, creation_timestamp, signature
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
                    creation_timestamp: row.get::<_, SqlDateTime>(5)?.0,
                    signature: {
                        let vec = row.get::<_, Vec<u8>>(6)?;
                        let arr: [u8; 64] = vec.try_into().map_err(|_| {
                            rusqlite::Error::FromSqlConversionFailure(
                                6,
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
        }).await.map_err(|_| MetadataDBError::Database(rusqlite::Error::ExecuteReturnedResults))??;

        // Send result through response channel
        if response_sender.send(Ok(result)).await.is_err() {
            error!("Failed to send piece repair history response");
        }

        Ok(())
    }

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
                    if let Err(e) = self
                        .handle_insert_object(&infohash_value, chunks_with_pieces, response_sender)
                        .await
                    {
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
            }
        }
    }
}

// tests for insertion and query functions in the metadatadb
#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use chrono::Utc;
    use subxt::ext::codec::Compact;
    use tempfile::NamedTempFile;

    use super::*;
    use crate::{
        metadata::models::{ChunkValue, InfohashValue, PieceValue},
        piece::PieceType,
    };

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

        // Create tables
        conn.execute(
            "CREATE TABLE infohashes (
                infohash BLOB PRIMARY KEY,
                name VARCHAR(4096) NOT NULL,
                length INTEGER NOT NULL,
                chunk_size INTEGER NOT NULL,
                chunk_count INTEGER NOT NULL,
                creation_timestamp DATETIME NOT NULL,
                signature BLOB NOT NULL
            )",
            [],
        )
        .expect("Failed to create infohashes table");

        conn.execute(
            "CREATE TABLE chunks (
                chunk_hash BLOB PRIMARY KEY,
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
        let infohash = [1u8; 32];
        let infohash_value = InfohashValue {
            infohash,
            name: "test_object".to_string(),
            length: 1000,
            chunk_size: 256,
            chunk_count: 4,
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0u8; 64]),
        };

        let chunk = ChunkValue {
            chunk_hash: [2u8; 32],
            k: 10,
            m: 5,
            chunk_size: 512,
            padlen: 0,
            original_chunk_size: 512,
        };

        let piece = PieceValue {
            piece_hash: [3u8; 32],
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

        // Create test data
        let infohash = [1u8; 32];
        let infohash_value = InfohashValue {
            infohash,
            name: "test_object".to_string(),
            length: 1000,
            chunk_size: 256,
            chunk_count: 4,
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0u8; 64]),
        };

        let chunk = ChunkValue {
            chunk_hash: [2u8; 32],
            k: 10,
            m: 5,
            chunk_size: 512,
            padlen: 0,
            original_chunk_size: 512,
        };

        let piece = PieceValue {
            piece_hash: [3u8; 32],
            piece_size: 256,
            piece_type: PieceType::Data,
            miners: vec![Compact(1), Compact(2)],
        };

        // Insert the object
        MetadataDB::insert_object(&command_sender, infohash_value, vec![(chunk, vec![piece])])
            .await
            .expect("Failed to insert object");

        // Query a random chunk
        let random_chunk = MetadataDB::get_random_chunk(&command_sender)
            .await
            .expect("Failed to get random chunk");
        assert_eq!(random_chunk.chunk_hash, [2u8; 32]);

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

        // Create test data
        let infohash = [1u8; 32];
        let infohash_value = InfohashValue {
            infohash,
            name: "test_object".to_string(),
            length: 1000,
            chunk_size: 256,
            chunk_count: 4,
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0u8; 64]),
        };

        let chunk = ChunkValue {
            chunk_hash: [2u8; 32],
            k: 10,
            m: 5,
            chunk_size: 512,
            padlen: 0,
            original_chunk_size: 512,
        };

        let piece = PieceValue {
            piece_hash: [3u8; 32],
            piece_size: 256,
            piece_type: PieceType::Data,
            miners: vec![Compact(1), Compact(2)],
        };

        // Insert the object
        MetadataDB::insert_object(&command_sender, infohash_value, vec![(chunk, vec![piece])])
            .await
            .expect("Failed to insert object");

        // Query the piece by its hash
        let queried_piece = MetadataDB::get_piece(&command_sender, [3u8; 32])
            .await
            .expect("Failed to get piece");
        assert_eq!(queried_piece.piece_hash, [3u8; 32]);
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
        let result = MetadataDB::get_piece(&command_sender, [99u8; 32]).await;

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

        // First create and insert a chunk that the piece repair history will reference
        let infohash = [1u8; 32];
        let infohash_value = InfohashValue {
            infohash,
            name: "test_object".to_string(),
            length: 1000,
            chunk_size: 256,
            chunk_count: 4,
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0u8; 64]),
        };

        let chunk = ChunkValue {
            chunk_hash: [2u8; 32],
            k: 10,
            m: 5,
            chunk_size: 512,
            padlen: 0,
            original_chunk_size: 512,
        };

        let piece = PieceValue {
            piece_hash: [3u8; 32],
            piece_size: 256,
            piece_type: PieceType::Data,
            miners: vec![Compact(1), Compact(2)],
        };

        // Insert the object first to create the chunk
        MetadataDB::insert_object(&command_sender, infohash_value, vec![(chunk, vec![piece])])
            .await
            .expect("Failed to insert object");

        // Create test data for piece repair history
        let piece_repair_hash = [4u8; 32];
        let piece_repair_history = PieceChallengeHistory {
            piece_repair_hash,
            piece_hash: [3u8; 32],
            chunk_hash: [2u8; 32], // References the chunk we just inserted
            validator_id: Compact(1),
            timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0u8; 64]),
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
        assert_eq!(queried_history.piece_hash, [3u8; 32]);
        assert_eq!(queried_history.chunk_hash, [2u8; 32]);
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

        // First create and insert a chunk that the chunk challenge history will reference
        let infohash = [1u8; 32];
        let infohash_value = InfohashValue {
            infohash,
            name: "test_object".to_string(),
            length: 1000,
            chunk_size: 256,
            chunk_count: 4,
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0u8; 64]),
        };

        let chunk = ChunkValue {
            chunk_hash: [2u8; 32],
            k: 10,
            m: 5,
            chunk_size: 512,
            padlen: 0,
            original_chunk_size: 512,
        };

        let piece = PieceValue {
            piece_hash: [3u8; 32],
            piece_size: 256,
            piece_type: PieceType::Data,
            miners: vec![Compact(1), Compact(2)],
        };

        // Insert the object first to create the chunk
        MetadataDB::insert_object(&command_sender, infohash_value, vec![(chunk, vec![piece])])
            .await
            .expect("Failed to insert object");

        // First insert a piece repair history that the chunk challenge history will reference
        let piece_repair_hash = [4u8; 32];
        let piece_repair_history = PieceChallengeHistory {
            piece_repair_hash,
            piece_hash: [3u8; 32],
            chunk_hash: [2u8; 32], // References the chunk we just inserted
            validator_id: Compact(1),
            timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0u8; 64]),
        };

        // Insert the piece repair history first
        MetadataDB::insert_piece_repair_history(&command_sender, piece_repair_history)
            .await
            .expect("Failed to insert piece repair history");

        // Create test data for chunk challenge history
        let challenge_hash = [5u8; 32];
        let chunk_challenge_history = ChunkChallengeHistory {
            challenge_hash,
            chunk_hash: [2u8; 32], // References the chunk we just inserted
            validator_id: Compact(1),
            miners_challenged: vec![Compact(1), Compact(2)],
            miners_successful: vec![Compact(1)],
            piece_repair_hash, // References the piece repair history we just inserted
            timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0u8; 64]),
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
        assert_eq!(queried_history.chunk_hash, [2u8; 32]);
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
        let infohash1 = [1u8; 32];
        let infohash_value1 = InfohashValue {
            infohash: infohash1,
            name: "test_object_1".to_string(),
            length: 1000,
            chunk_size: 256,
            chunk_count: 1,
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0u8; 64]),
        };

        let chunk1 = ChunkValue {
            chunk_hash: [2u8; 32],
            k: 10,
            m: 5,
            chunk_size: 512,
            padlen: 0,
            original_chunk_size: 512,
        };

        let piece1 = PieceValue {
            piece_hash: [3u8; 32], // Same piece hash for both objects
            piece_size: 256,
            piece_type: PieceType::Data,
            miners: vec![Compact(1), Compact(2)], // Initial miners
        };

        // Insert the first object
        MetadataDB::insert_object(
            &command_sender,
            infohash_value1,
            vec![(chunk1, vec![piece1])],
        )
        .await
        .expect("Failed to insert first object");

        // Verify initial miners
        let initial_pieces = MetadataDB::get_pieces_by_chunk(&command_sender, [2u8; 32])
            .await
            .expect("Failed to get pieces by chunk");
        assert_eq!(initial_pieces.len(), 1);
        assert_eq!(initial_pieces[0].miners, vec![Compact(1), Compact(2)]);

        // Create second object with the SAME piece but different miners [2, 3, 4]
        let infohash2 = [4u8; 32];
        let infohash_value2 = InfohashValue {
            infohash: infohash2,
            name: "test_object_2".to_string(),
            length: 2000,
            chunk_size: 256,
            chunk_count: 1,
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([1u8; 64]),
        };

        let chunk2 = ChunkValue {
            chunk_hash: [5u8; 32], // Different chunk
            k: 10,
            m: 5,
            chunk_size: 512,
            padlen: 0,
            original_chunk_size: 512,
        };

        let piece2 = PieceValue {
            piece_hash: [3u8; 32], // SAME piece hash as piece1
            piece_size: 256,
            piece_type: PieceType::Data,
            miners: vec![Compact(2), Compact(3), Compact(4)], // Some overlap (2) and new miners (3, 4)
        };

        // Insert the second object - this should update the miners for the existing piece
        MetadataDB::insert_object(
            &command_sender,
            infohash_value2,
            vec![(chunk2, vec![piece2])],
        )
        .await
        .expect("Failed to insert second object");

        // Query the piece from both chunks to verify miners were updated
        let pieces_chunk1 = MetadataDB::get_pieces_by_chunk(&command_sender, [2u8; 32])
            .await
            .expect("Failed to get pieces from first chunk");

        let pieces_chunk2 = MetadataDB::get_pieces_by_chunk(&command_sender, [5u8; 32])
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

        // Test edge case: inserting the same piece again with duplicate miners should not change anything
        let infohash3 = [6u8; 32];
        let infohash_value3 = InfohashValue {
            infohash: infohash3,
            name: "test_object_3".to_string(),
            length: 3000,
            chunk_size: 256,
            chunk_count: 1,
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([2u8; 64]),
        };

        let chunk3 = ChunkValue {
            chunk_hash: [7u8; 32], // Different chunk
            k: 10,
            m: 5,
            chunk_size: 512,
            padlen: 0,
            original_chunk_size: 512,
        };

        let piece3 = PieceValue {
            piece_hash: [3u8; 32], // SAME piece hash again
            piece_size: 256,
            piece_type: PieceType::Data,
            miners: vec![Compact(2), Compact(3)], // Miners that already exist
        };

        // Insert the third object - should not change miners since they already exist
        MetadataDB::insert_object(
            &command_sender,
            infohash_value3,
            vec![(chunk3, vec![piece3])],
        )
        .await
        .expect("Failed to insert third object");

        // Verify miners are still the same
        let pieces_chunk3 = MetadataDB::get_pieces_by_chunk(&command_sender, [7u8; 32])
            .await
            .expect("Failed to get pieces from third chunk");

        let mut actual_miners_chunk3 = pieces_chunk3[0].miners.clone();
        actual_miners_chunk3.sort_by_key(|c| c.0);

        assert_eq!(actual_miners_chunk3, expected_sorted);

        // Abort the background task
        handle.abort();
        // temp_file will be automatically cleaned up when it goes out of scope
    }
}
