use std::io;

#[derive(Debug)]
pub struct Error {
    msg: String,
}

impl Error {
    pub fn new(msg: String) -> Self {
        return Self { msg }
    }
}

impl From<waste_island::Error> for Error {
    fn from(value: waste_island::Error) -> Self {
        Self { msg: value.to_string() }
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self { msg: value.to_string() }
    }
}

impl From<hyper::Error> for Error {
    fn from(value: hyper::Error) -> Self {
        Self { msg: value.to_string() }
    }
}
