use std::fmt::{self, Display, Formatter};
use std::io::Write;
use std::ops::{Deref, Index};
use std::slice;

use crate::piece::PieceHash;

pub struct PieceHashStr(String);

impl PieceHashStr {
    pub fn new(piece_hash: String) -> Result<Self, String> {
        if piece_hash.len() != 64 {
            return Err(format!(
                "The piece hash {piece_hash} is not 64 characters long"
            ));
        }

        Ok(Self(piece_hash))
    }
}

impl<I> Index<I> for PieceHashStr
where
    I: slice::SliceIndex<str>,
{
    type Output = I::Output;

    #[inline]
    fn index(&self, index: I) -> &I::Output {
        &self.0[index]
    }
}

impl Deref for PieceHashStr {
    type Target = String;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<PieceHashStr> for String {
    #[inline]
    fn from(val: PieceHashStr) -> Self {
        val.0
    }
}

impl Display for PieceHashStr {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&**self, f)
    }
}

pub fn piecehash_str_to_bytes(piecehash: &PieceHashStr) -> Result<PieceHash, String> {
    let final_piece_hash = hex::decode(piecehash.as_str())
        .map_err(|err| format!("Failed to decode piecehash into hex: {err}"))?;
    let mut piece_hash_bytes = [0u8; 32];
    let mut w: &mut [u8] = &mut piece_hash_bytes;
    w.write_all(&final_piece_hash)
        .map_err(|err| format!("Failed to write bytes: {err}"))?;

    Ok(piece_hash_bytes)
}
