use serde::{Deserialize, Serialize};

/// Represents a piece that is stored on miners (storage nodes).
#[derive(Clone, Debug)]
pub struct Piece {
    /// The hash of the piece, which acts as a unique identifier for the piece.
    // pub piece_hash: PieceHash,

    /// The size of the piece in bytes.
    pub piece_size: u64,

    /// Whether it is a data or parity piece.
    pub piece_type: PieceType,

    /// The index of the piece within the chunk.
    pub piece_idx: u64,

    /// The index of the chunk this piece belongs to.
    pub chunk_idx: u64,

    /// The data contained in the piece.
    pub data: Vec<u8>,
}

pub struct PieceResponse {
    pub piece_size: u64,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PieceType {
    Data = 0,
    Parity = 1,
}

impl TryFrom<u8> for PieceType {
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(PieceType::Data),
            1 => Ok(PieceType::Parity),
            _ => Err("Invalid PieceType value"),
        }
    }
}
impl From<PieceType> for u8 {
    fn from(piece_type: PieceType) -> Self {
        match piece_type {
            PieceType::Data => 0,
            PieceType::Parity => 1,
        }
    }
}
