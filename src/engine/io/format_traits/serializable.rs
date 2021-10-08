//! Serializes a given struct to a given ByteMut

use bytes::{BufMut, Bytes, BytesMut};

use crate::constants::PAGE_SIZE;

pub trait Serializable {
    /// Transforms the structure to a byte stream
    fn serialize(&self, buffer: &mut impl BufMut);

    /// Produces a new page to support the change to how the I/O subsystem works
    fn serialize_and_pad(&self) -> Bytes {
        let mut page = BytesMut::with_capacity(PAGE_SIZE as usize);
        self.serialize(&mut page);

        if page.len() != PAGE_SIZE as usize {
            let padding = vec![0; PAGE_SIZE as usize - page.len()];
            page.extend_from_slice(&padding);
        }

        page.freeze()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Buf;

    use super::*;

    struct Test {
        inner: u32,
    }
    impl Serializable for Test {
        fn serialize(&self, buffer: &mut impl BufMut) {
            buffer.put_u32_le(self.inner);
        }
    }

    #[tokio::test]
    async fn test_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let test = Test { inner: 2000 };

        let mut buffer = test.serialize_and_pad();

        assert_eq!(buffer.len(), PAGE_SIZE as usize);
        assert_eq!(test.inner, buffer.get_u32_le());

        Ok(())
    }
}
