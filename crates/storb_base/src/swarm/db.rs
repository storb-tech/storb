use std::path::{Path, PathBuf};

use crabtensor::sign::KeypairSignature;
use rusqlite::{params, Connection, OptionalExtension, Result};
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
    InvalidPath(String),
    MissingTable(String),
    ExtensionLoad(String),
}

impl From<rusqlite::Error> for MetadataDBError {
    fn from(err: rusqlite::Error) -> Self {
        MetadataDBError::Database(err)
    }
}

pub enum MetadataDBCommand {
    InsertPiece(PieceValue, [u8; 32]),
    InsertChunk(ChunkValue, [u8; 32]),
    InsertInfohash(InfohashValue),
    InsertPieceRepairHistory(PieceChallengeHistory),
    InsertChunkChallengeHistory(ChunkChallengeHistory),
    GetPiecesByChunk([u8; 32]),
    GetChunksByInfohash(Vec<u8>),
    GetInfohash([u8; 32]),
    GetPieceRepairHistory([u8; 32]),
    GetChunkChallengeHistory([u8; 32]),
}

pub struct MetadataDB {
    conn: Connection,
    crsqlite_lib: PathBuf,
    db_path: PathBuf,
    command_sender: mpsc::Sender<MetadataDBCommand>,
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

        let conn = Connection::open(db_path)?;
        Self::load_crsqlite_extension(&conn, crsqlite_lib)?;

        let (command_sender, command_receiver) =
            mpsc::channel::<MetadataDBCommand>(DB_MPSC_BUFFER_SIZE);

        let db = Self {
            conn,
            crsqlite_lib: crsqlite_lib.to_path_buf(),
            db_path: db_path.to_path_buf(),
            command_sender,
            command_receiver,
        };

        db.validate_database()?;

        let command_sender_clone = db.command_sender.clone();

        Ok((db, command_sender_clone))
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
    fn validate_database(&self) -> Result<(), MetadataDBError> {
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
            let exists: bool = self.conn.query_row(
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

    pub fn insert_piece(
        &self,
        piece: PieceValue,
        chunk_hash: &[u8; 32],
    ) -> Result<(), MetadataDBError> {
        let tx = self.conn.unchecked_transaction()?;

        // convert piece.miners into json string if it's not already
        let miners = serde_json::to_string(&piece.miners).map_err(|e| {
            MetadataDBError::Database(rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::new(e),
            ))
        })?;

        tx.execute(
            "INSERT INTO pieces (piece_hash, validator_id, chunk_idx, piece_idx, piece_size, piece_type, miners) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                piece.piece_hash,
                piece.validator_id.0,
                piece.chunk_idx,
                piece.piece_idx,
                piece.piece_size,
                piece.piece_type as u8,
                miners
            ],
        )?;

        tx.execute(
            "INSERT INTO chunk_pieces (chunk_hash, piece_idx, piece_hash) VALUES (?1, ?2, ?3)",
            params![chunk_hash, piece.piece_idx, piece.piece_hash],
        )?;

        tx.commit()?;
        Ok(())
    }

    pub fn insert_chunk(
        &self,
        chunk: ChunkValue,
        infohash: &[u8; 32],
    ) -> Result<(), MetadataDBError> {
        let tx = self.conn.unchecked_transaction()?;

        tx.execute("INSERT INTO chunks (chunk_hash, chunk_idx, k, m, chunk_size, padlen, original_chunk_size) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)", params![
            chunk.chunk_hash,
            chunk.chunk_idx,
            chunk.k,
            chunk.m,
            chunk.chunk_size,
            chunk.padlen,
            chunk.original_chunk_size
        ])?;

        tx.execute(
            "INSERT INTO tracker_chunks (infohash, chunk_idx, chunk_hash) VALUES (?1, ?2, ?3)",
            params![infohash, chunk.chunk_idx, chunk.chunk_hash],
        )?;

        tx.commit()?;
        Ok(())
    }

    pub fn insert_infohash(&self, infohash_value: &InfohashValue) -> Result<(), MetadataDBError> {
        let tx = self.conn.unchecked_transaction()?;

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

        tx.commit()?;
        Ok(())
    }

    pub fn insert_piece_repair_history(
        &self,
        value: &PieceChallengeHistory,
    ) -> Result<(), MetadataDBError> {
        let tx = self.conn.unchecked_transaction()?;

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
        Ok(())
    }

    pub fn insert_chunk_challenge_history(
        &self,
        value: &ChunkChallengeHistory,
    ) -> Result<(), MetadataDBError> {
        let tx = self.conn.unchecked_transaction()?;

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
        Ok(())
    }

    // Inserts all the metadata for the object in bulk
    // TODO: Consider renaming function?
    pub fn insert_object(
        &self,
        infohash_value: &InfohashValue,
        chunks_with_pieces: Vec<(ChunkValue, Vec<PieceValue>)>,
    ) -> Result<(), MetadataDBError> {
        let tx = self.conn.unchecked_transaction()?;

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
        Ok(())
    }

    /// Processes db events and commands.
    ///
    /// This asynchronous function continuously processes db events and commands,
    /// handling inserts, updates, and queries as they come in.
    pub async fn process_events(&mut self) {
        while let Some(command) = self.command_receiver.recv().await {
            match command {
                MetadataDBCommand::InsertPiece(piece, chunk_hash) => {
                    if let Err(e) = self.insert_piece(piece, &chunk_hash) {
                        error!("Error inserting piece: {:?}", e);
                    }
                }
                MetadataDBCommand::InsertChunk(chunk, infohash) => {
                    if let Err(e) = self.insert_chunk(chunk, &infohash) {
                        error!("Error inserting chunk: {:?}", e);
                    }
                }
                MetadataDBCommand::InsertInfohash(infohash_value) => {
                    if let Err(e) = self.insert_infohash(&infohash_value) {
                        error!("Error inserting infohash: {:?}", e);
                    }
                }
                MetadataDBCommand::InsertPieceRepairHistory(value) => {
                    if let Err(e) = self.insert_piece_repair_history(&value) {
                        error!("Error inserting piece repair history: {:?}", e);
                    }
                }
                MetadataDBCommand::InsertChunkChallengeHistory(value) => {
                    if let Err(e) = self.insert_chunk_challenge_history(&value) {
                        error!("Error inserting chunk challenge history: {:?}", e);
                    }
                }
                MetadataDBCommand::GetPiecesByChunk(chunk_hash) => {
                    if let Ok(pieces) = self.get_pieces_by_chunk(&chunk_hash) {
                        debug!("Retrieved pieces: {:?}", pieces);
                    } else {
                        error!("Error retrieving pieces for chunk: {:?}", chunk_hash);
                    }
                }
                MetadataDBCommand::GetChunksByInfohash(infohash) => {
                    if let Ok(chunks) = self.get_chunks_by_infohash(&infohash) {
                        debug!("Retrieved chunks: {:?}", chunks);
                    } else {
                        error!("Error retrieving chunks for infohash: {:?}", infohash);
                    }
                }
                MetadataDBCommand::GetInfohash(infohash) => {
                    if let Ok(infohash_value) = self.get_infohash(&infohash) {
                        debug!("Retrieved infohash: {:?}", infohash_value);
                    } else {
                        error!("Error retrieving infohash: {:?}", infohash);
                    }
                }
                MetadataDBCommand::GetPieceRepairHistory(piece_repair_hash) => {
                    if let Ok(history) = self.get_piece_repair_history(&piece_repair_hash) {
                        debug!("Retrieved piece repair history: {:?}", history);
                    } else {
                        error!(
                            "Error retrieving piece repair history: {:?}",
                            piece_repair_hash
                        );
                    }
                }
                MetadataDBCommand::GetChunkChallengeHistory(challenge_hash) => {
                    if let Ok(history) = self.get_chunk_challenge_history(&challenge_hash) {
                        debug!("Retrieved chunk challenge history: {:?}", history);
                    } else {
                        error!(
                            "Error retrieving chunk challenge history: {:?}",
                            challenge_hash
                        );
                    }
                }
            }
        }
    }

    pub fn get_pieces_by_chunk(
        &self,
        chunk_hash: &[u8; 32],
    ) -> Result<Vec<PieceValue>, MetadataDBError> {
        let mut stmt = self.conn.prepare(
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

        Ok(pieces)
    }

    pub fn get_chunks_by_infohash(
        &self,
        infohash: &[u8],
    ) -> Result<Vec<ChunkValue>, MetadataDBError> {
        let mut stmt = self.conn.prepare(
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
    }

    pub fn get_infohash(&self, infohash: &[u8; 32]) -> Result<InfohashValue, MetadataDBError> {
        let mut stmt = self.conn.prepare(
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
                // TODO: create a newtype for KeypairSignature?
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

        Ok(infohash_value)
    }

    pub fn get_piece_repair_history(
        &self,
        piece_repair_hash: &[u8; 32],
    ) -> Result<PieceChallengeHistory, MetadataDBError> {
        let mut stmt = self.conn.prepare(
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

        Ok(piece_challenge_history)
    }

    pub fn get_chunk_challenge_history(
        &self,
        challenge_hash: &[u8; 32],
    ) -> Result<ChunkChallengeHistory, MetadataDBError> {
        let mut stmt = self.conn.prepare(
            "SELECT challenge_hash, chunk_hash, validator_id, miners_challenged, miners_successful, piece_repair_hash, timestamp, signature 
             FROM chunk_challenge_history 
             WHERE challenge_hash = ?1",
        )?;

        let chunk_challenge_history = stmt.query_row(params![challenge_hash], |row| {
            Ok(ChunkChallengeHistory {
                challenge_hash: row.get(0)?,
                chunk_hash: row.get(1)?,
                validator_id: Compact(row.get::<_, u16>(2)?),
                miners_challenged: serde_json::from_str(&row.get::<_, String>(3)?).map_err(
                    |e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            3,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    },
                )?,
                miners_successful: serde_json::from_str(&row.get::<_, String>(4)?).map_err(
                    |e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            4,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    },
                )?,
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

        Ok(chunk_challenge_history)
    }
}

// tests for insertion and query functions in the metadatadb
#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;
    use std::path::PathBuf;

    use chrono::Utc;
    use subxt::ext::codec::Compact;

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

    #[test]
    fn test_insert_and_query_infohash() {
        let (db_path, crsqlite_lib) = setup_test_db();
        let (db, _) = MetadataDB::new(&db_path, &crsqlite_lib).unwrap();

        let infohash_value = InfohashValue {
            infohash: [0u8; 32],
            length: 1000,
            chunk_size: 256,
            chunk_count: 4,
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0u8; 64]),
        };

        db.insert_infohash(&infohash_value).unwrap();
        let queried_infohash = db.get_infohash(&infohash_value.infohash).unwrap();

        assert_eq!(queried_infohash, infohash_value);

        // Clean up test file
        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_insert_and_query_chunk() {
        let (db_path, crsqlite_lib) = setup_test_db();
        let (db, _) = MetadataDB::new(&db_path, &crsqlite_lib).unwrap();

        // Create an infohash value first
        let infohash = [2u8; 32];
        let infohash_value = InfohashValue {
            infohash,
            length: 1000,
            chunk_size: 256,
            chunk_count: 4,
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0u8; 64]),
        };

        // Insert the infohash first to satisfy foreign key constraint
        db.insert_infohash(&infohash_value).unwrap();

        let chunk_value = ChunkValue {
            chunk_hash: [1u8; 32],
            chunk_idx: 0,
            k: 10,
            m: 5,
            chunk_size: 512,
            padlen: 0,
            original_chunk_size: 512,
        };

        db.insert_chunk(chunk_value.clone(), &infohash).unwrap();
        let chunks = db.get_chunks_by_infohash(&infohash).unwrap();

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], chunk_value);

        // Clean up test file
        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_insert_and_query_piece() {
        let (db_path, crsqlite_lib) = setup_test_db();
        let (db, _) = MetadataDB::new(&db_path, &crsqlite_lib).unwrap();

        // Create and insert an infohash first
        let infohash = [21u8; 32];
        let infohash_value = InfohashValue {
            infohash,
            length: 1000,
            chunk_size: 256,
            chunk_count: 4,
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0u8; 64]),
        };
        db.insert_infohash(&infohash_value).unwrap();

        // Create and insert the chunk first
        let chunk_hash = [4u8; 32];
        let chunk = ChunkValue {
            chunk_hash,
            chunk_idx: 0,
            k: 10,
            m: 5,
            chunk_size: 512,
            padlen: 0,
            original_chunk_size: 512,
        };
        db.insert_chunk(chunk, &infohash).unwrap();

        // Now create and insert the piece
        let piece_value = PieceValue {
            piece_hash: [3u8; 32],
            validator_id: Compact(1),
            chunk_idx: 0,
            piece_idx: 0,
            piece_size: 256,
            piece_type: PieceType::Data,
            miners: vec![Compact(1), Compact(2)],
        };

        db.insert_piece(piece_value.clone(), &chunk_hash).unwrap();
        let pieces = db.get_pieces_by_chunk(&chunk_hash).unwrap();
        assert_eq!(pieces.len(), 1);
        assert_eq!(pieces[0], piece_value);

        // Clean up test file
        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_insert_and_query_piece_repair_history() {
        let (db_path, crsqlite_lib) = setup_test_db();
        let (db, _) = MetadataDB::new(&db_path, &crsqlite_lib).unwrap();

        // First create and insert a chunk
        let chunk = ChunkValue {
            chunk_hash: [7u8; 32],
            chunk_idx: 0,
            k: 10,
            m: 5,
            chunk_size: 512,
            padlen: 0,
            original_chunk_size: 512,
        };

        // Create and insert an infohash (needed for foreign key)
        let infohash = [20u8; 32];
        let infohash_value = InfohashValue {
            infohash,
            length: 1000,
            chunk_size: 256,
            chunk_count: 4,
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0u8; 64]),
        };

        // Insert infohash first
        db.insert_infohash(&infohash_value).unwrap();

        // Insert the chunk
        db.insert_chunk(chunk, &infohash).unwrap();

        // Now create and insert a piece
        let piece = PieceValue {
            piece_hash: [6u8; 32],
            validator_id: Compact(1),
            chunk_idx: 0,
            piece_idx: 0,
            piece_size: 256,
            piece_type: PieceType::Data,
            miners: vec![Compact(1), Compact(2)],
        };
        db.insert_piece(piece, &[7u8; 32]).unwrap();

        // Finally create and insert the piece repair history
        let piece_repair_history = PieceChallengeHistory {
            piece_repair_hash: [5u8; 32],
            piece_hash: [6u8; 32],
            chunk_hash: [7u8; 32],
            validator_id: Compact(1),
            timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0u8; 64]),
        };

        db.insert_piece_repair_history(&piece_repair_history)
            .unwrap();
        let queried_history = db
            .get_piece_repair_history(&piece_repair_history.piece_repair_hash)
            .unwrap();

        assert_eq!(queried_history, piece_repair_history);

        // Clean up test file
        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_insert_and_query_chunk_challenge_history() {
        let (db_path, crsqlite_lib) = setup_test_db();
        let (db, _) = MetadataDB::new(&db_path, &crsqlite_lib).unwrap();

        // 1. Create an infohash
        let infohash = [22u8; 32];
        let infohash_value = InfohashValue {
            infohash,
            length: 1000,
            chunk_size: 256,
            chunk_count: 4,
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0u8; 64]),
        };
        db.insert_infohash(&infohash_value).unwrap();

        // 2. Create a chunk
        let chunk = ChunkValue {
            chunk_hash: [9u8; 32],
            chunk_idx: 0,
            k: 10,
            m: 5,
            chunk_size: 512,
            padlen: 0,
            original_chunk_size: 512,
        };
        db.insert_chunk(chunk, &infohash).unwrap();

        // 3. Create a piece
        let piece = PieceValue {
            piece_hash: [23u8; 32],
            validator_id: Compact(1),
            chunk_idx: 0,
            piece_idx: 0,
            piece_size: 256,
            piece_type: PieceType::Data,
            miners: vec![Compact(1), Compact(2)],
        };
        db.insert_piece(piece, &[9u8; 32]).unwrap();

        // 4. Create a piece repair history
        let piece_repair_history = PieceChallengeHistory {
            piece_repair_hash: [10u8; 32],
            piece_hash: [23u8; 32],
            chunk_hash: [9u8; 32],
            validator_id: Compact(1),
            timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0u8; 64]),
        };
        db.insert_piece_repair_history(&piece_repair_history)
            .unwrap();

        // 5. Finally create the chunk challenge history
        let chunk_challenge_history = ChunkChallengeHistory {
            challenge_hash: [8u8; 32],
            chunk_hash: [9u8; 32],
            validator_id: Compact(1),
            miners_challenged: vec![Compact(1), Compact(2)],
            miners_successful: vec![Compact(2)],
            piece_repair_hash: [10u8; 32],
            timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0u8; 64]),
        };

        db.insert_chunk_challenge_history(&chunk_challenge_history)
            .unwrap();
        let queried_history = db
            .get_chunk_challenge_history(&chunk_challenge_history.challenge_hash)
            .unwrap();

        assert_eq!(queried_history, chunk_challenge_history);

        // Clean up test file
        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_insert_and_query_object() {
        let (db_path, crsqlite_lib) = setup_test_db();
        let (db, _) = MetadataDB::new(&db_path, &crsqlite_lib).unwrap();

        let infohash_value = InfohashValue {
            infohash: [11u8; 32],
            length: 2000,
            chunk_size: 512,
            chunk_count: 4,
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0u8; 64]),
        };

        let chunk_value = ChunkValue {
            chunk_hash: [12u8; 32],
            chunk_idx: 0,
            k: 10,
            m: 5,
            chunk_size: 512,
            padlen: 0,
            original_chunk_size: 512,
        };

        let piece_value = PieceValue {
            piece_hash: [13u8; 32],
            validator_id: Compact(1),
            chunk_idx: 0,
            piece_idx: 0,
            piece_size: 256,
            piece_type: PieceType::Data,
            miners: vec![Compact(1), Compact(2)],
        };

        db.insert_object(
            &infohash_value,
            vec![(chunk_value.clone(), vec![piece_value.clone()])],
        )
        .unwrap();

        let queried_infohash = db.get_infohash(&infohash_value.infohash).unwrap();
        assert_eq!(queried_infohash, infohash_value);

        let chunks = db.get_chunks_by_infohash(&infohash_value.infohash).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], chunk_value);

        let pieces = db.get_pieces_by_chunk(&chunk_value.chunk_hash).unwrap();
        assert_eq!(pieces.len(), 1);
        assert_eq!(pieces[0], piece_value);

        // Clean up test file
        cleanup_test_db(&db_path);
    }
}
