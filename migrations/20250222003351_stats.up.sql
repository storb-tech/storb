-- Add up migration script here
CREATE TABLE miner_stats (
    miner_uid INTEGER PRIMARY KEY,
    challenge_successes INTEGER,
    challenge_attempts INTEGER,
    retrieval_successes INTEGER,
    retrieval_attempts INTEGER,
    store_successes INTEGER,
    store_attempts INTEGER,
    total_successes INTEGER
)
