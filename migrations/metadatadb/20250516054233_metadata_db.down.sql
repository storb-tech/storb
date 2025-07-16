-- Drop indices

DROP INDEX IF EXISTS idx_chunks_by_hash;

DROP INDEX IF EXISTS idx_pieces_by_hash;

DROP INDEX IF EXISTS idx_chunks_ref_count;

DROP INDEX IF EXISTS idx_pieces_ref_count;

DROP INDEX IF EXISTS idx_chunk_pieces_by_chunk;

DROP INDEX IF EXISTS idx_object_chunks_by_info_hash;

DROP INDEX IF EXISTS idx_objects_by_owner;

DROP INDEX IF EXISTS idx_piece_repair_by_piece;

DROP INDEX IF EXISTS idx_chunk_challenge_by_chunk;

DROP INDEX IF EXISTS idx_account_nonces_timestamp;

DROP INDEX IF EXISTS idx_miner_pieces_by_miner;

DROP INDEX IF EXISTS idx_miner_pieces_by_piece;

DROP INDEX IF EXISTS idx_pieces_to_repair;

-- Drop tables

DROP TABLE IF EXISTS account_nonces;

DROP TABLE IF EXISTS chunk_challenge_history;

DROP TABLE IF EXISTS piece_repair_history;

DROP TABLE IF EXISTS miner_pieces;

DROP TABLE IF EXISTS chunk_pieces;

DROP TABLE IF EXISTS pieces;

DROP TABLE IF EXISTS object_chunks;

DROP TABLE IF EXISTS chunks;

DROP TABLE IF EXISTS objects;
