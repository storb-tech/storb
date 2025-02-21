use std::fmt::{self, Display, Formatter};
use std::io::Write;
use std::ops::{Deref, Index};
use std::slice;

pub struct PieceHash(String);

impl PieceHash {
    pub fn new(piece_hash: String) -> Result<Self, String> {
        if piece_hash.len() != 64 {
            return Err(format!(
                "The piece hash {piece_hash} is not 64 characters long"
            ));
        }

        Ok(Self(piece_hash))
    }
}

impl<I> Index<I> for PieceHash
where
    I: slice::SliceIndex<str>,
{
    type Output = I::Output;

    #[inline]
    fn index(&self, index: I) -> &I::Output {
        &self.0[index]
    }
}

impl Deref for PieceHash {
    type Target = String;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<PieceHash> for String {
    #[inline]
    fn from(val: PieceHash) -> Self {
        val.0
    }
}

impl Display for PieceHash {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&**self, f)
    }
}

pub fn piecehash_to_bytes_raw(piecehash: &PieceHash) -> Result<[u8; 32], String> {
    // let final_piece_hash = piecehash.as_bytes();
    let final_piece_hash = hex::decode(piecehash.as_str())
        .map_err(|err| format!("Failed to decode piecehash into hex: {err}"))?;
    let mut piece_hash_bytes = [0u8; 32];
    let mut w: &mut [u8] = &mut piece_hash_bytes;
    w.write_all(&final_piece_hash)
        .map_err(|err| format!("Failed to write bytes: {err}"))?;

    Ok(piece_hash_bytes)
}
