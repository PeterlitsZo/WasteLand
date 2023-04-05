#[derive(Debug)]
pub struct Error {
    message: String,
}

impl Error {
    pub fn new(message: &str) -> Self {
        Error {
            message: message.to_string(),
        }
    }
}