-- Add down migration script here

DROP INDEX IF EXISTS idx_chunks_by_hash;
DROP INDEX IF EXISTS idx_pieces_by_hash;
DROP INDEX IF EXISTS idx_pieces_validator;

DROP TABLE IF EXISTS chunk_challenge_history;
DROP TABLE IF EXISTS piece_repair_history;
DROP TABLE IF EXISTS chunk_pieces;
DROP TABLE IF EXISTS tracker_chunks;
DROP TABLE IF EXISTS pieces;
DROP TABLE IF EXISTS chunks;
DROP TABLE IF EXISTS trackers;
