//! Defining if something is null or not so I'm not using a bool everywhere

use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Nullable {
    Null,
    NotNull,
}

impl From<bool> for Nullable {
    fn from(b: bool) -> Self {
        if b {
            Nullable::Null
        } else {
            Nullable::NotNull
        }
    }
}

impl From<u8> for Nullable {
    fn from(u: u8) -> Self {
        if u == 0x0 {
            Nullable::Null
        } else {
            Nullable::NotNull
        }
    }
}

impl fmt::Display for Nullable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Nullable::NotNull => write!(f, "NotNull"),
            Nullable::Null => write!(f, "Null"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nullable_display() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(Nullable::NotNull.to_string(), "NotNull".to_string());
        assert_eq!(Nullable::Null.to_string(), "Null".to_string());
        Ok(())
    }

    #[test]
    fn test_nullable_from() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(Nullable::from(false), Nullable::NotNull);
        assert_eq!(Nullable::from(true), Nullable::Null);

        assert_eq!(Nullable::from(0), Nullable::Null);
        for u in 1..u8::MAX {
            assert_eq!(Nullable::from(u), Nullable::NotNull);
        }

        Ok(())
    }
}
