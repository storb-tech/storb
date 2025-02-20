use std::num::ParseIntError;
use std::result::Result;
use std::str::FromStr;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum VersionError {
    #[error(transparent)]
    ParseIntError(#[from] ParseIntError),
    #[error("The version string `{0}` has too many parts, needs to be in the form `x.y.z`")]
    TooManyParts(String),
}

/// The version of Storb.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Version {
    pub major: u16,
    pub minor: u16,
    pub patch: u16,
}

/// For getting the spec version (u64) from the Version.
/// This will allow for a version `x.yyy.zz` to be converted to `xyyyzz`.
impl From<&Version> for u64 {
    fn from(version: &Version) -> Self {
        (version.major as u64) * 100_000 + (version.minor as u64) * 100 + (version.patch as u64)
    }
}

/// For getting the Version from a &str.
impl FromStr for Version {
    type Err = VersionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts: Vec<u16> = vec![];
        for part in s.split('.') {
            parts.push(part.parse()?);
        }

        let mut parts_iter = parts.into_iter();

        let major = parts_iter.next().unwrap_or(0);
        let minor = parts_iter.next().unwrap_or(0);
        let patch = parts_iter.next().unwrap_or(0);

        if parts_iter.next().is_some() {
            return Err(VersionError::TooManyParts(s.to_string()));
        }

        Ok(Version {
            major,
            minor,
            patch,
        })
    }
}
