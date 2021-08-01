//! Set of utility functions I've written a few times, but not enough (yet) to break into a submodule.
use bytes::{Buf, BufMut, BytesMut};
use thiserror::Error;

/// Will provide the length in bytes the supplied usize will encode to without encoding
pub fn expected_encoded_size(size: usize) -> usize {
    //Discussion here: https://github.com/rust-lang/rfcs/issues/2844
    (size + 127 - 1) / 127
}

/// Writes a length out to a byte stream as a series of 7 bit numbers, with the high
/// bit used to indicate we have hit the end of the length
pub fn encode_size(buffer: &mut impl BufMut, mut size: usize) {
    while size > 0 {
        let last_count = size as u8;
        let mut digit: u8 = last_count & 0x7f;
        size >>= 7;
        if size > 0 {
            digit |= 0x80;
        }
        buffer.put_u8(digit);
    }
}

pub fn parse_size(buffer: &mut impl Buf) -> Result<usize, SizeError> {
    let mut size: usize = 0;
    let mut high_bit = 1;
    let mut loop_count = 0;
    while high_bit == 1 {
        if !buffer.has_remaining() {
            return Err(SizeError::BufferTooShort());
        }

        let b = buffer.get_u8();
        high_bit = b >> 7;

        let mut low_bits: usize = (b & 0x7f).into();
        low_bits <<= 7 * loop_count;
        loop_count += 1;

        size = size
            .checked_add(low_bits)
            .ok_or_else(SizeError::SizeOverflow)?;
    }

    Ok(size)
}

#[derive(Debug, Error)]
pub enum SizeError {
    #[error("Buffer too short to parse")]
    BufferTooShort(),
    #[error("Size Overflow!")]
    SizeOverflow(),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let test = 1;

        let mut buffer = BytesMut::with_capacity(expected_encoded_size(test));
        encode_size(&mut buffer, test);
        let mut serialized = buffer.freeze();

        assert_eq!(serialized.len(), expected_encoded_size(test));
        let parsed = parse_size(&mut serialized)?;
        assert_eq!(test, parsed);

        let test = 128;

        let mut buffer = BytesMut::with_capacity(expected_encoded_size(test));
        encode_size(&mut buffer, test);
        let mut serialized = buffer.freeze();

        assert_eq!(serialized.len(), expected_encoded_size(test));
        let parsed = parse_size(&mut serialized)?;
        assert_eq!(test, parsed);

        Ok(())
    }
}
