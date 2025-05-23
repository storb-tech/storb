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
            "INSERT INTO pieces (piece_hash, validator_id, chunk_idx, piece_idx, piece_size, piece_type) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                piece.piece_hash,
                piece.validator_id,
                piece.chunk_idx,
                piece.piece_idx,
                piece.piece_size,
                piece.piece_type as i32,
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

    pub fn insert_infohash(infohash_value: &InfohashValue) -> Result<(), MetadataDBError> {
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
}
