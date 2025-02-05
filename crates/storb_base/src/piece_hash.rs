use std::fmt::{self, Display, Formatter};
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
    fn from(val: PieceHash) -> String {
        val.0
    }
}

impl Display for PieceHash {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&**self, f)
    }
}
