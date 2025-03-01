-- Table for miner stats --
CREATE TABLE miner_stats (
    miner_uid INTEGER PRIMARY KEY,
    challenge_successes INTEGER DEFAULT 0,
    challenge_attempts INTEGER DEFAULT 0,
    retrieval_successes INTEGER DEFAULT 0,
    retrieval_attempts INTEGER DEFAULT 0,
    store_successes INTEGER DEFAULT 0,
    store_attempts INTEGER DEFAULT 0,
    total_successes INTEGER DEFAULT 0
);

WITH RECURSIVE numbers AS (
    SELECT 0 AS value
    UNION ALL
    SELECT value + 1
    FROM numbers
    WHERE value < 255
)

INSERT INTO miner_stats (miner_uid)
SELECT value FROM numbers;

-- Table for miner chunk values --
CREATE TABLE chunks (
    chunk_hash BLOB PRIMARY KEY, -- chunk hash (RecordKey)
    validator_id INTEGER,
    piece_hashes BLOB, -- serialized Rust vector (Vec<[u8; 32]>)
    chunk_idx INTEGER,
    k INTEGER,
    m INTEGER,
    chunk_size INTEGER,
    padlen INTEGER,
    original_chunk_size INTEGER,
    signature BLOB -- KeypairSignature
);
