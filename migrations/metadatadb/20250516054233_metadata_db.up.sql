-- Add up migration script here

-- infohashes
CREATE TABLE IF NOT EXISTS infohashes (
  infohash            BLOB            PRIMARY KEY NOT NULL DEFAULT '',
  name                VARCHAR(4096)   NOT NULL DEFAULT 'default',
  length              INTEGER         NOT NULL DEFAULT 0,
  chunk_size          INTEGER         NOT NULL DEFAULT 0,
  chunk_count         INTEGER         NOT NULL DEFAULT 0,
  owner_account_id    BLOB            NOT NULL DEFAULT '',
  creation_timestamp  DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  signature           TEXT            NOT NULL DEFAULT ''
);

-- Chunks
CREATE TABLE IF NOT EXISTS chunks (
  chunk_hash           BLOB      PRIMARY KEY NOT NULL DEFAULT '',
  k                    INTEGER   NOT NULL DEFAULT 0,
  m                    INTEGER   NOT NULL DEFAULT 0,
  chunk_size           INTEGER   NOT NULL DEFAULT 0,
  padlen               INTEGER   NOT NULL DEFAULT 0,
  original_chunk_size  INTEGER   NOT NULL DEFAULT 0,
  ref_count            INTEGER   NOT NULL DEFAULT 1
);

-- Chunk‑tracker mapping 
CREATE TABLE IF NOT EXISTS tracker_chunks (
  infohash   BLOB    NOT NULL DEFAULT '',
  chunk_idx  INTEGER NOT NULL DEFAULT 0,
  chunk_hash BLOB    NOT NULL DEFAULT '',
  PRIMARY KEY (infohash, chunk_idx)
);

-- Pieces
CREATE TABLE IF NOT EXISTS pieces (
  piece_hash   BLOB      PRIMARY KEY NOT NULL DEFAULT '',
  piece_size   INTEGER   NOT NULL DEFAULT 0,
  piece_type   INTEGER   NOT NULL DEFAULT 0, -- 0: Data, 1: Parity
  miners       TEXT      NOT NULL DEFAULT '', -- JSON array of miner IDs
  ref_count    INTEGER   NOT NULL DEFAULT 1
);

-- Piece‑chunk mapping 
CREATE TABLE IF NOT EXISTS chunk_pieces (
  chunk_hash  BLOB    NOT NULL DEFAULT '',
  piece_idx   INTEGER NOT NULL DEFAULT 0,
  piece_hash  BLOB    NOT NULL DEFAULT '',
  PRIMARY KEY (chunk_hash, piece_idx)
);

-- Piece‑repair history
CREATE TABLE IF NOT EXISTS piece_repair_history (
  piece_repair_hash  BLOB      PRIMARY KEY NOT NULL DEFAULT '',
  piece_hash         BLOB      NOT NULL DEFAULT '',
  chunk_hash         BLOB      NOT NULL DEFAULT '',
  validator_id       INTEGER   NOT NULL DEFAULT 0,
  timestamp          DATETIME  NOT NULL DEFAULT CURRENT_TIMESTAMP,
  signature          TEXT      NOT NULL DEFAULT ''
);

-- Chunk‑challenge history
CREATE TABLE IF NOT EXISTS chunk_challenge_history (
  challenge_hash     BLOB      PRIMARY KEY NOT NULL DEFAULT '',
  chunk_hash         BLOB      NOT NULL DEFAULT '',
  validator_id       INTEGER   NOT NULL DEFAULT 0,
  miners_challenged  TEXT      NOT NULL DEFAULT '', -- JSON array of miner IDs
  miners_successful  TEXT      NOT NULL DEFAULT '', -- JSON array of miner IDs
  piece_repair_hash  BLOB,
  timestamp          DATETIME  NOT NULL DEFAULT CURRENT_TIMESTAMP,
  signature          TEXT      NOT NULL DEFAULT ''
);

-- Nonce tracking to prevent replay attacks
CREATE TABLE IF NOT EXISTS account_nonces (
  account_id         BLOB      NOT NULL,
  nonce              BLOB      NOT NULL,
  timestamp          DATETIME  NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (account_id, nonce)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_chunks_by_hash     ON chunks(chunk_hash);
CREATE INDEX IF NOT EXISTS idx_pieces_by_hash     ON pieces(piece_hash);
CREATE INDEX IF NOT EXISTS idx_chunks_ref_count ON chunks(ref_count);
CREATE INDEX IF NOT EXISTS idx_pieces_ref_count ON pieces(ref_count);
CREATE INDEX IF NOT EXISTS idx_chunk_pieces_by_chunk ON chunk_pieces(chunk_hash);
CREATE INDEX IF NOT EXISTS idx_tracker_chunks_by_infohash ON tracker_chunks(infohash);
CREATE INDEX IF NOT EXISTS idx_infohashes_by_owner ON infohashes(owner_account_id);
CREATE INDEX IF NOT EXISTS idx_piece_repair_by_piece ON piece_repair_history(piece_hash);
CREATE INDEX IF NOT EXISTS idx_chunk_challenge_by_chunk ON chunk_challenge_history(chunk_hash);
CREATE INDEX IF NOT EXISTS idx_account_nonces_timestamp ON account_nonces(timestamp);
