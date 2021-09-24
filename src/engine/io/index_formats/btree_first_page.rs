use crate::engine::io::{
    format_traits::{Parseable, Serializable},
    page_formats::{PageOffset, PageOffsetError},
};
use bytes::BufMut;
use thiserror::Error;

/// Special page that points to where the root page of the index is really located
#[derive(Debug, PartialEq)]
pub struct BTreeFirstPage {
    pub root_offset: PageOffset,
}

impl Parseable<BTreeFirstPageError> for BTreeFirstPage {
    type Output = Self;
    fn parse(buffer: &mut impl bytes::Buf) -> Result<Self::Output, BTreeFirstPageError> {
        let root_offset = PageOffset::parse(buffer)?;
        Ok(BTreeFirstPage { root_offset })
    }
}

impl Serializable for BTreeFirstPage {
    fn serialize(&self, buffer: &mut impl BufMut) {
        self.root_offset.serialize(buffer);
    }
}

#[derive(Debug, Error)]
pub enum BTreeFirstPageError {
    #[error(transparent)]
    PageOffsetError(#[from] PageOffsetError),
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use crate::constants::PAGE_SIZE;

    use super::*;

    #[test]
    fn test_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let first = BTreeFirstPage {
            root_offset: PageOffset(1),
        };

        let mut buffer = BytesMut::with_capacity(PAGE_SIZE as usize);
        first.serialize(&mut buffer);

        let result = BTreeFirstPage::parse(&mut buffer)?;

        assert_eq!(first, result);

        Ok(())
    }
}
