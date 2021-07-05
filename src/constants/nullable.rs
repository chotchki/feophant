//! Defining if something is null or not so I'm not using a bool everywhere

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Nullable {
    Null,
    NotNull,
}

impl From<bool> for Nullable {
    fn from(b: bool) -> Self {
        if b {
            return Nullable::Null;
        } else {
            return Nullable::NotNull;
        }
    }
}

impl From<u8> for Nullable {
    fn from(u: u8) -> Self {
        if u == 0x0 {
            return Nullable::Null;
        } else {
            return Nullable::NotNull;
        }
    }
}
