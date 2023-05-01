use std::fmt::{Display, Debug};

use crate::error::Error;

pub const HASH_SIZE: usize = 32;

#[derive(Eq, PartialEq, Hash, PartialOrd, Clone, Copy)]
pub struct Hash([u8; HASH_SIZE]);

impl Hash {
    pub fn from_str(str: &str) -> Result<Self, Error> {
        if str.len() != HASH_SIZE * 2 {
            return Err(Error::new("the length of str is not equal to HASH_SIZE * 2"));
        }

        let mut result = [0u8; HASH_SIZE];
        for i in 0..HASH_SIZE {
            let byte = u8::from_str_radix(&str[2 * i..2 * i + 2], 16).unwrap();
            result[i] = byte;
        }
        Ok(Self(result))
    }

    pub fn from_bytes(bytes: [u8; HASH_SIZE]) -> Self {
        Self(bytes)
    }
}

impl Display for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for i in 0..HASH_SIZE {
            write!(f, "{:02x}", self.0[i])?;
        }
        Ok(())
    }
}

impl Debug for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)?;
        Ok(())
    }
}