use std::sync::Arc;

use anyhow::Result;
use rusqlite::{Connection, DatabaseName};
use subxt::ext::sp_core::hexdisplay::AsBytesRef;
use tokio::sync::Mutex;
use tracing::info;

use crate::swarm::models;

pub struct MemoryDb {
    pub conn: Arc<Mutex<Connection>>,
    pub db_path: String,
}

impl MemoryDb {
    pub async fn new(db_path: &str) -> Result<Self> {
        // Create in-memory connection
        let conn_arc = Arc::new(Mutex::new(Connection::open_in_memory()?));

        let mut conn = conn_arc.lock().await;
        conn.restore(DatabaseName::Main, db_path, Some(|_p| ()))?;

        Ok(Self {
            conn: conn_arc.clone(),
            db_path: db_path.to_string(),
        })
    }

    pub async fn run_backup(&self) {
        let db_path = self.db_path.clone();
        let conn = self.conn.clone();

        info!("Backing up database");
        // Preview memory database
        {
            let conn = conn.lock().await;
            conn.backup(DatabaseName::Main, &db_path, None).unwrap();
        }
        info!("Database backup complete");
    }
}

// Inserts chunk DHT value into the SQLite DB
pub async fn insert_chunk_dht_value(
    chunk_dht_value: models::ChunkDHTValue,
    db_conn: Arc<Mutex<Connection>>,
) -> Result<()> {
    let conn = db_conn.lock().await;
    let chunk_hash = chunk_dht_value.chunk_hash.as_ref();
    let vali_id = chunk_dht_value.validator_id.0 as i64;
    let serialized_piece_hashes = bincode::serialize(&chunk_dht_value.piece_hashes)?;

    let mut stmt = conn.prepare(
        "INSERT OR REPLACE INTO chunks (chunk_hash, validator_id, piece_hashes, chunk_idx, k, m, chunk_size, padlen, original_chunk_size, signature) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )?;
    stmt.execute((
        chunk_hash,
        vali_id,
        serialized_piece_hashes,
        chunk_dht_value.chunk_idx as i64,
        chunk_dht_value.k as i64,
        chunk_dht_value.m as i64,
        chunk_dht_value.chunk_size as i64,
        chunk_dht_value.padlen as i64,
        chunk_dht_value.original_chunk_size as i64,
        chunk_dht_value.signature.as_bytes_ref(),
    ))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Once;

    use tempfile::NamedTempFile;
    // use tokio::time::sleep;

    // This runs before any tests
    // static INIT: Once = Once::new();

    // fn setup_logging() {
    //     INIT.call_once(|| {
    //         tracing_subscriber::fmt()
    //             .with_max_level(tracing::Level::DEBUG)
    //             .with_test_writer() // This ensures output goes to the test console
    //             .init();
    //     });
    // }

    // helper function to initialize the disk database
    fn init_disk_db(db_path: &str) -> Result<()> {
        let conn = Connection::open(db_path)?;
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", [])?;
        conn.execute("INSERT INTO test (value) VALUES (?1)", ["test_value"])?;
        Ok(())
    }

    #[tokio::test]
    async fn test_memory_db_creation() -> Result<()> {
        // Create a temporary file for testing
        let temp_file = NamedTempFile::new()?;
        let db_path = temp_file.path().to_str().unwrap();

        // Create an initial SQLite database with some test data
        init_disk_db(db_path)?;

        // Create MemoryDb instance
        let memory_db = MemoryDb::new(db_path).await?;

        // Verify data was loaded correctly
        let conn = memory_db.conn.lock().await;
        let mut stmt = conn.prepare("SELECT value FROM test WHERE id = 1")?;
        let value: String = stmt.query_row([], |row| row.get(0))?;

        assert_eq!(value, "test_value");
        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_db_path() {
        let result = MemoryDb::new("/nonexistent/path/db.sqlite").await;
        assert!(result.is_err());
    }
}
