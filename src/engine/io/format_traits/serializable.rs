//! Serializes a given struct to a given ByteMut

use bytes::{BufMut, Bytes, BytesMut};
use tokio::sync::OwnedRwLockWriteGuard;

use crate::constants::PAGE_SIZE;

pub trait Serializable {
    /// Transforms the structure to a byte stream
    fn serialize(&self, buffer: &mut impl BufMut);

    /// Produces a new page to support the change to how the I/O subsystem works
    fn serialize_and_pad(&self, buffer: &mut OwnedRwLockWriteGuard<Option<Bytes>>) {
        let mut page = BytesMut::with_capacity(PAGE_SIZE as usize);
        self.serialize(&mut page);

        if page.len() != PAGE_SIZE as usize {
            let padding = vec![0; PAGE_SIZE as usize - page.len()];
            page.extend_from_slice(&padding);
        }

        buffer.replace(page.freeze());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Buf;
    use tokio::sync::RwLock;

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

        let page_lock = Arc::new(RwLock::new(None));
        let mut guard = page_lock.clone().write_owned().await;

        test.serialize_and_pad(&mut guard);
        drop(guard);

        let page = page_lock.read_owned().await;
        if let Some(s) = page.as_ref() {
            let mut s = s.clone();
            assert_eq!(s.len(), PAGE_SIZE as usize);
            assert_eq!(test.inner, s.get_u32_le());
        } else {
            panic!("None found!");
        }

        Ok(())
    }
}
