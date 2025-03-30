use anyhow::Result;
use chrono::{DateTime, NaiveDateTime, Utc};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiKey {
    pub key: String,
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub rate_limit: Option<u32>,     // Requests per minute
    pub upload_limit: Option<u64>,   // Max total upload bytes
    pub download_limit: Option<u64>, // Max total download bytes
    pub upload_used: u64,
    pub download_used: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiKeyConfig {
    pub name: String,
    pub expires_at: Option<DateTime<Utc>>,
    pub rate_limit: Option<u32>,
    pub upload_limit: Option<u64>,
    pub download_limit: Option<u64>,
}

#[derive(Debug)]
pub struct ApiUsageLog {
    pub key: String,
    pub endpoint: String,
    pub timestamp: DateTime<Utc>,
    pub upload_bytes: u64,
    pub download_bytes: u64,
}

#[derive(Debug)]
pub struct ApiKeyManager {
    conn: Arc<Mutex<Connection>>,
}

impl ApiKeyManager {
    pub fn new(db_path: PathBuf) -> Result<Self> {
        let conn = Connection::open(db_path)?;

        // Create API keys table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS api_keys (
                key TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER,
                rate_limit INTEGER,
                upload_limit INTEGER,
                download_limit INTEGER,
                upload_used INTEGER NOT NULL DEFAULT 0,
                download_used INTEGER NOT NULL DEFAULT 0
            )",
            [],
        )?;

        // Create usage logs table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS api_usage_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT NOT NULL,
                endpoint TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                upload_bytes INTEGER NOT NULL DEFAULT 0,
                download_bytes INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY(key) REFERENCES api_keys(key) ON DELETE CASCADE
            )",
            [],
        )?;

        // Create index for efficient rate limit checking
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_usage_logs_key_timestamp 
             ON api_usage_logs(key, timestamp)",
            [],
        )?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub async fn create_key(&self, config: ApiKeyConfig) -> Result<ApiKey> {
        let key = format!("storb_{}", Uuid::new_v4());
        let now = Utc::now();

        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO api_keys (
                key, name, created_at, expires_at, rate_limit, 
                upload_limit, download_limit, upload_used, download_used
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0)",
            params![
                key,
                config.name,
                now.timestamp(),
                config.expires_at.map(|d| d.timestamp()),
                config.rate_limit,
                config.upload_limit,
                config.download_limit,
            ],
        )?;

        Ok(ApiKey {
            key,
            name: config.name,
            created_at: now,
            expires_at: config.expires_at,
            rate_limit: config.rate_limit,
            upload_limit: config.upload_limit,
            download_limit: config.download_limit,
            upload_used: 0,
            download_used: 0,
        })
    }

    pub async fn validate_key(&self, key: &str) -> Result<Option<ApiKey>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare("SELECT * FROM api_keys WHERE key = ?")?;

        let key_row = stmt.query_row([key], |row| {
            Ok(ApiKey {
                key: row.get(0)?,
                name: row.get(1)?,
                created_at: DateTime::from_naive_utc_and_offset(
                    NaiveDateTime::from_timestamp_opt(row.get(2)?, 0).unwrap(),
                    Utc,
                ),
                expires_at: row.get::<_, Option<i64>>(3)?.map(|ts| {
                    DateTime::from_naive_utc_and_offset(
                        NaiveDateTime::from_timestamp_opt(ts, 0).unwrap(),
                        Utc,
                    )
                }),
                rate_limit: row.get(4)?,
                upload_limit: row.get(5)?,
                download_limit: row.get(6)?,
                upload_used: row.get(7)?,
                download_used: row.get(8)?,
            })
        });

        match key_row {
            Ok(key) => {
                // Check if key has expired
                if let Some(expires_at) = key.expires_at {
                    if expires_at < Utc::now() {
                        return Ok(None);
                    }
                }
                Ok(Some(key))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn update_usage(
        &self,
        key: &str,
        upload_bytes: u64,
        download_bytes: u64,
    ) -> Result<()> {
        let conn = self.conn.lock().await;
        conn.execute(
            "UPDATE api_keys SET 
                upload_used = upload_used + ?,
                download_used = download_used + ?
            WHERE key = ?",
            params![upload_bytes, download_bytes, key],
        )?;
        Ok(())
    }

    pub async fn list_keys(&self) -> Result<Vec<ApiKey>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare("SELECT * FROM api_keys")?;
        let keys = stmt.query_map([], |row| {
            Ok(ApiKey {
                key: row.get(0)?,
                name: row.get(1)?,
                created_at: DateTime::from_naive_utc_and_offset(
                    NaiveDateTime::from_timestamp_opt(row.get(2)?, 0).unwrap(),
                    Utc,
                ),
                expires_at: row.get::<_, Option<i64>>(3)?.map(|ts| {
                    DateTime::from_naive_utc_and_offset(
                        NaiveDateTime::from_timestamp_opt(ts, 0).unwrap(),
                        Utc,
                    )
                }),
                rate_limit: row.get(4)?,
                upload_limit: row.get(5)?,
                download_limit: row.get(6)?,
                upload_used: row.get(7)?,
                download_used: row.get(8)?,
            })
        })?;

        let mut result = Vec::new();
        for key in keys {
            result.push(key?);
        }
        Ok(result)
    }

    pub async fn delete_key(&self, key: &str) -> Result<bool> {
        let conn = self.conn.lock().await;
        let rows = conn.execute("DELETE FROM api_keys WHERE key = ?", [key])?;
        Ok(rows > 0)
    }

    pub async fn check_quota(
        &self,
        key: &str,
        upload_bytes: u64,
        download_bytes: u64,
    ) -> Result<bool> {
        let api_key = match self.validate_key(key).await? {
            Some(key) => key,
            None => return Ok(false),
        };

        // Check upload quota
        if let Some(upload_limit) = api_key.upload_limit {
            if api_key.upload_used + upload_bytes > upload_limit {
                return Ok(false);
            }
        }

        // Check download quota
        if let Some(download_limit) = api_key.download_limit {
            if api_key.download_used + download_bytes > download_limit {
                return Ok(false);
            }
        }

        Ok(true)
    }

    pub async fn check_rate_limit(&self, key: &str, rate_limit: u32) -> Result<bool> {
        let conn = self.conn.lock().await;
        let one_minute_ago = (Utc::now() - chrono::Duration::minutes(1)).timestamp();

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM api_usage_logs 
             WHERE key = ? AND timestamp > ?",
            params![key, one_minute_ago],
            |row| row.get(0),
        )?;

        Ok(count + 1 < rate_limit as i64) // +1 for the current request
    }

    pub async fn log_api_usage(
        &self,
        key: &str,
        endpoint: &str,
        upload_bytes: u64,
        download_bytes: u64,
    ) -> Result<()> {
        let conn = self.conn.lock().await;
        let now = Utc::now();

        conn.execute(
            "INSERT INTO api_usage_logs (
                key, endpoint, timestamp, upload_bytes, download_bytes
            ) VALUES (?, ?, ?, ?, ?)",
            params![key, endpoint, now.timestamp(), upload_bytes, download_bytes],
        )?;

        Ok(())
    }

    // Add cleanup method for old logs
    pub async fn cleanup_old_logs(&self, days_to_keep: i64) -> Result<()> {
        let conn = self.conn.lock().await;
        let cutoff = (Utc::now() - chrono::Duration::days(days_to_keep)).timestamp();

        conn.execute(
            "DELETE FROM api_usage_logs WHERE timestamp < ?",
            params![cutoff],
        )?;

        Ok(())
    }
}
