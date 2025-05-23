use std::path::{Path, PathBuf};

use rusqlite::{params, Connection, OptionalExtension, Result};

use super::models::{ChunkChallengeHistory, ChunkValue, InfohashValue, PieceChallengeHistory};
use crate::swarm::models::PieceValue;

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

pub struct MetadataDB {
    conn: Connection,
    crsqlite_lib: PathBuf,
    db_path: PathBuf,
}

impl MetadataDB {
    pub fn new(db_path: &Path, crsqlite_lib: &Path) -> Result<Self, MetadataDBError> {
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

        let db = Self {
            conn,
            crsqlite_lib: crsqlite_lib.to_path_buf(),
            db_path: db_path.to_path_buf(),
        };

        db.validate_database()?;
        Ok(db)
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

        tx.execute(
            "INSERT INTO pieces (piece_hash, validator_id, chunk_idx, piece_idx, piece_size, piece_type, miners) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                piece.piece_hash,
                piece.validator_id,
                piece.chunk_idx,
                piece.piece_idx,
                piece.piece_size,
                piece.piece_type as i32,
                piece.miners,
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
                infohash_value.creation_timestamp,
                infohash_value.signature
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
                value.validator_id,
                value.timestamp,
                value.signature
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

        tx.execute(
            "INSERT INTO chunk_challenge_history (challenge_hash, chunk_hash, validator_id, miners_challenged, miners_successful, piece_repair_hash, timestamp, signature) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                value.challenge_hash,
                value.chunk_hash,
                value.validator_id,
                value.miners_challenged,
                value.miners_successful,
                value.piece_repair_hash,
                value.timestamp,
                value.signature
            ],
        )?;

        tx.commit()?;
        Ok(())
    }

    // Inserts all the metadata for the object in bulk
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
                infohash_value.creation_timestamp,
                infohash_value.signature
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
                // Insert piece
                piece_stmt.execute(params![
                    piece.piece_hash,
                    piece.validator_id,
                    piece.chunk_idx,
                    piece.piece_idx,
                    piece.piece_size,
                    piece.piece_type as i32,
                    piece.miners
                ])?;

                // Insert chunk-piece mapping
                chunk_pieces_stmt.execute(params![
                    chunk.chunk_hash,
                    piece.piece_idx,
                    piece.piece_hash
                ])?;
            }
        }

        tx.commit()?;
        Ok(())
    }

    pub fn get_pieces_by_chunk(
        &self,
        chunk_hash: &[u8; 32],
    ) -> Result<Vec<PieceValue>, MetadataDBError> {
        let mut stmt = self.conn.prepare(
            "SELECT p.piece_hash, p.validator_id, p.chunk_idx, p.piece_idx, p.piece_size, p.piece_type, p.miners 
                FROM pieces p 
                INNER JOIN chunk_pieces cp ON p.piece_hash = cp.piece_hash 
                WHERE cp.chunk_hash = ?1",
        )?;

        let pieces = stmt
            .query_map(params![chunk_hash], |row| {
                Ok(PieceValue {
                    piece_hash: row.get(0)?,
                    validator_id: row.get(1)?,
                    chunk_idx: row.get(2)?,
                    piece_idx: row.get(3)?,
                    piece_size: row.get(4)?,
                    piece_type: row.get::<_, i32>(5)?.try_into()?,
                    miners: row.get(6)?,
                })
            })?
            .collect();

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
                creation_timestamp: row.get(4)?,
                signature: row.get(5)?,
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
                validator_id: row.get(3)?,
                timestamp: row.get(4)?,
                signature: row.get(5)?,
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
                validator_id: row.get(2)?,
                miners_challenged: row.get(3)?,
                miners_successful: row.get(4)?,
                piece_repair_hash: row.get(5)?,
                timestamp: row.get(6)?,
                signature: row.get(7)?,
            })
        })?;

        Ok(chunk_challenge_history)
    }
}
