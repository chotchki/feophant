//! Serializes a given struct to a given ByteMut

use bytes::{BufMut, BytesMut};

use crate::constants::PAGE_SIZE;

pub trait Serializable {
    /// Transforms the structure to a byte stream
    fn serialize(&self, buffer: &mut impl BufMut);

    /// Handles updating the page from the I/O sub system
    fn serialize_and_pad(&self, page: &mut Option<BytesMut>) {
        match page.as_mut() {
            Some(mut s) => {
                s.clear();

                self.serialize(&mut s);

                if s.len() != PAGE_SIZE as usize {
                    let padding = vec![0; PAGE_SIZE as usize - s.len()];
                    s.extend_from_slice(&padding);
                }
            }
            None => {
                let mut new_page = BytesMut::with_capacity(PAGE_SIZE as usize);
                self.serialize(&mut new_page);

                if new_page.len() != PAGE_SIZE as usize {
                    let padding = vec![0; PAGE_SIZE as usize - new_page.len()];
                    new_page.extend_from_slice(&padding);
                }

                page.replace(new_page);
            }
        }
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

    #[test]
    fn test_none() -> Result<(), Box<dyn std::error::Error>> {
        let test = Test { inner: 2000 };

        let mut page = None;
        test.serialize_and_pad(&mut page);

        assert!(page.is_some());

        let mut page = page.unwrap();
        assert_eq!(page.len(), PAGE_SIZE as usize);
        assert_eq!(test.inner, page.get_u32_le());

        Ok(())
    }

    #[test]
    fn test_some() -> Result<(), Box<dyn std::error::Error>> {
        let test = Test { inner: 2000 };

        let mut page = Some(BytesMut::with_capacity(PAGE_SIZE as usize));
        test.serialize_and_pad(&mut page);

        assert!(page.is_some());

        let mut page = page.unwrap();
        assert_eq!(page.len(), PAGE_SIZE as usize);
        assert_eq!(test.inner, page.get_u32_le());

        Ok(())
    }
}
