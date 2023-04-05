#[macro_export]
macro_rules! try_or_return_error {
    ($result:expr, $message_prefix:expr) => {
        match $result {
            Ok(r) => r,
            Err(e) => return Err(Error::new(&format!("{}: {}", $message_prefix, e))),
        }
    };
}

pub const HASH_LENGTH: usize = 32;
pub const OFFSET_LENGTH: usize = 8;

pub fn offset_usize_to_bytes(n: usize) -> [u8; OFFSET_LENGTH] {
    let mut bytes = [0u8; 8];
    for i in 0..8 {
        bytes[i] = (n >> (i * 8)) as u8;
    }
    bytes
}

pub fn offset_bytes_to_usize(bytes: [u8; OFFSET_LENGTH]) -> usize {
    let mut n = 0usize;
    for i in 0..8 {
        n |= (bytes[i] as usize) << (i * 8);
    }
    n
}

pub fn hash_bytes_to_string(bytes: &[u8; HASH_LENGTH]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{:02x}", byte))
        .collect::<String>()
}

pub fn hash_string_to_bytes(s: &str) -> [u8; HASH_LENGTH] {
    let mut result = [0u8; HASH_LENGTH];
    for i in 0..HASH_LENGTH {
        let byte = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap();
        result[i] = byte;
    }
    result
}
