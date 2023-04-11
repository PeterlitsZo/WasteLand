use std::fmt::Display;

use crate::error::Error;

pub const HASH_LENGTH: usize = 32;

#[derive(PartialEq, PartialOrd)]
pub struct Hash([u8; HASH_LENGTH]);

#[derive(PartialEq, PartialOrd)]
pub struct RefHash<'a>(&'a [u8; HASH_LENGTH]);

impl Hash {
    pub fn from_str(str: &str) -> Result<Self, Error> {
        if str.len() != HASH_LENGTH * 2 {
            return Err(Error::new("the length of str is not equal to HASH_LENGTH * 2"));
        }

        let mut result = [0u8; HASH_LENGTH];
        for i in 0..HASH_LENGTH {
            let byte = u8::from_str_radix(&str[2 * i..2 * i + 2], 16).unwrap();
            result[i] = byte;
        }
        Ok(Self(result))
    }

    pub fn from_bytes(bytes: [u8; HASH_LENGTH]) -> Self {
        Self(bytes)
    }

    pub fn to_bytes(self) -> [u8; HASH_LENGTH] {
        return self.0
    }

    pub fn as_ref<'a>(&'a self) -> RefHash<'a> {
        return RefHash(&self.0)
    }
}

impl Display for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for i in 0..HASH_LENGTH {
            write!(f, "{:02x}", self.0[i])?;
        }
        Ok(())
    }
}

impl<'a> RefHash<'a> {
    pub fn from_bytes_ref(bytes_ref: &'a [u8; HASH_LENGTH]) -> Self {
        Self(bytes_ref)
    }

    pub fn to_bytes_ref(&self) -> &'a [u8; HASH_LENGTH] {
        self.0
    }
}