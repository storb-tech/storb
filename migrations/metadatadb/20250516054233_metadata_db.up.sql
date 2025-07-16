-- The metadata database schema

-- Object data
CREATE TABLE IF NOT EXISTS objects (
  info_hash          BLOB            PRIMARY KEY NOT NULL DEFAULT '',
  name              VARCHAR(4096)   NOT NULL DEFAULT 'default',
  length            INTEGER         NOT NULL DEFAULT 0,
  chunk_size        INTEGER         NOT NULL DEFAULT 0,
  chunk_count       INTEGER         NOT NULL DEFAULT 0,
  owner_account_id  BLOB            NOT NULL DEFAULT '',
  signature         TEXT            NOT NULL DEFAULT '',
  created_at        DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Chunks, the subdivisions of objects
CREATE TABLE IF NOT EXISTS chunks (
  chunk_hash           BLOB      PRIMARY KEY NOT NULL DEFAULT '',
  encoding_type        INTEGER   NOT NULL DEFAULT 0,  -- 0: None, 1: Zfec, 2: RaptorQ
  k                    INTEGER   NOT NULL DEFAULT 0,
  m                    INTEGER   NOT NULL DEFAULT 0,
  chunk_size           INTEGER   NOT NULL DEFAULT 0,
  pad_length           INTEGER   NOT NULL DEFAULT 0,
  original_chunk_size  INTEGER   NOT NULL DEFAULT 0, -- Size before padding
  ref_count            INTEGER   NOT NULL DEFAULT 1,
  created_at           DATETIME  NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at           DATETIME  NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Object-to-chunk mapping
CREATE TABLE IF NOT EXISTS object_chunks (
  info_hash   BLOB    NOT NULL DEFAULT '',
  chunk_idx  INTEGER NOT NULL DEFAULT 0,
  chunk_hash BLOB    NOT NULL DEFAULT '',
  PRIMARY KEY (info_hash, chunk_idx)
);

-- Pieces, the erasure-encoded blocks of chunks
CREATE TABLE IF NOT EXISTS pieces (
  piece_hash   BLOB      PRIMARY KEY NOT NULL DEFAULT '',
  piece_size   INTEGER   NOT NULL DEFAULT 0,
  piece_type   INTEGER   NOT NULL DEFAULT 0,  -- 0: Data, 1: Parity
  miners       TEXT      NOT NULL DEFAULT '', -- JSON array of miner IDs
  ref_count    INTEGER   NOT NULL DEFAULT 1,
  created_at   DATETIME  NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at   DATETIME  NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Piece-to‑chunk mapping 
CREATE TABLE IF NOT EXISTS chunk_pieces (
  chunk_hash  BLOB    NOT NULL DEFAULT '',
  piece_idx   INTEGER NOT NULL DEFAULT 0,
  piece_hash  BLOB    NOT NULL DEFAULT '',
  PRIMARY KEY (chunk_hash, piece_idx)
);

-- A mapping of miner UIDs to the pieces they store
CREATE TABLE IF NOT EXISTS miner_pieces (
  miner_uid    INTEGER   NOT NULL,
  piece_hash   BLOB      NOT NULL,
  UNIQUE (miner_uid, piece_hash)
);

-- Table of pieces to repair, and the miner uids which failed to serve them --
CREATE TABLE IF NOT EXISTS pieces_to_repair (
    piece_hash BLOB, -- piece hash
    miners TEXT NOT NULL DEFAULT '[]', -- JSON array of miner IDs who failed to serve the piece
    UNIQUE (piece_hash, miners)
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

CREATE INDEX IF NOT EXISTS idx_chunks_by_hash ON chunks(chunk_hash);

CREATE INDEX IF NOT EXISTS idx_pieces_by_hash ON pieces(piece_hash);

CREATE INDEX IF NOT EXISTS idx_chunks_ref_count ON chunks(ref_count);

CREATE INDEX IF NOT EXISTS idx_pieces_ref_count ON pieces(ref_count);

CREATE INDEX IF NOT EXISTS idx_chunk_pieces_by_chunk ON chunk_pieces(chunk_hash);

CREATE INDEX IF NOT EXISTS idx_object_chunks_by_info_hash ON object_chunks(info_hash);

CREATE INDEX IF NOT EXISTS idx_objects_by_owner ON objects(owner_account_id);

CREATE INDEX IF NOT EXISTS idx_piece_repair_by_piece ON piece_repair_history(piece_hash);

CREATE INDEX IF NOT EXISTS idx_chunk_challenge_by_chunk ON chunk_challenge_history(chunk_hash);

CREATE INDEX IF NOT EXISTS idx_account_nonces_timestamp ON account_nonces(timestamp);

CREATE INDEX IF NOT EXISTS idx_miner_pieces_by_miner ON miner_pieces(miner_uid);

CREATE INDEX IF NOT EXISTS idx_miner_pieces_by_piece ON miner_pieces(piece_hash);

CREATE INDEX IF NOT EXISTS idx_pieces_to_repair ON pieces_to_repair(piece_hash);
