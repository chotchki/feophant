use crate::{
    constants::{PAGES_PER_FILE, PAGE_SIZE},
    engine::io::ConstEncodedSize,
};
use std::{fmt, mem::size_of, num::TryFromIntError, ops::AddAssign};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PageOffset(pub usize);

impl PageOffset {
    /// This will calculate a page offset based on the max file count and the offset from the first
    /// non-zero page in a file.
    ///
    /// Example: found file blah_blah.1 and in that file found a single non-zero page.
    ///     We will return a page offset of 2 * PAGES_PER_FILE + 1
    pub fn calculate_page_offset(file_number: usize, offset_in_file: usize) -> PageOffset {
        let offset = file_number * PAGES_PER_FILE + (offset_in_file / PAGE_SIZE as usize);
        PageOffset(offset)
    }

    /// Gets the needed size for this offset to support resize operations
    pub fn get_file_chunk_size(&self) -> usize {
        ((self.0 % PAGES_PER_FILE) + 1) * PAGE_SIZE as usize
    }
    /// Gets the file number for use in opening the file chunk
    pub fn get_file_number(&self) -> usize {
        self.0 / PAGES_PER_FILE
    }

    /// Gets the location to seek to in order to write to the block the page offset points at
    pub fn get_file_seek(&self) -> usize {
        self.get_file_chunk_size() - PAGE_SIZE as usize
    }

    /// Determines if a given offset will be the same file or not
    pub fn is_same_file(&self, rhs: &PageOffset) -> bool {
        let diff;
        if self.0 > rhs.0 {
            diff = self.0 - rhs.0;
        } else {
            diff = rhs.0 - self.0;
        }
        PAGES_PER_FILE > diff
    }

    /// Gets the next offset in sequence
    pub fn next(&self) -> PageOffset {
        PageOffset(self.0 + 1)
    }
}

impl AddAssign for PageOffset {
    fn add_assign(&mut self, other: Self) {
        self.0.add_assign(other.0);
    }
}

impl ConstEncodedSize for PageOffset {
    fn encoded_size() -> usize {
        size_of::<usize>()
    }
}

impl fmt::Display for PageOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Error)]
pub enum PageOffsetError {
    #[error(transparent)]
    TryFromIntError(#[from] TryFromIntError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_assign() -> Result<(), Box<dyn std::error::Error>> {
        let mut test = PageOffset(1);
        test += PageOffset(2);
        assert_eq!(test, PageOffset(3));
        Ok(())
    }

    #[test]
    fn test_calculate_page_offset() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(PageOffset::calculate_page_offset(0, 0), PageOffset(0));

        assert_eq!(
            PageOffset::calculate_page_offset(1, PAGE_SIZE as usize),
            PageOffset(PAGES_PER_FILE + 1)
        );

        Ok(())
    }

    #[test]
    fn test_get_file_chunk_size() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(PageOffset(0).get_file_chunk_size(), PAGE_SIZE as usize);
        assert_eq!(PageOffset(1).get_file_chunk_size(), PAGE_SIZE as usize * 2);
        assert_eq!(
            PageOffset(PAGES_PER_FILE).get_file_chunk_size(),
            PAGE_SIZE as usize
        );
        assert_eq!(
            PageOffset(PAGES_PER_FILE - 1).get_file_chunk_size(),
            PAGE_SIZE as usize * PAGES_PER_FILE
        );
        assert_eq!(
            PageOffset(PAGES_PER_FILE + 1).get_file_chunk_size(),
            2 * PAGE_SIZE as usize
        );

        Ok(())
    }

    #[test]
    fn test_get_file_number() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(PageOffset(0).get_file_number(), 0);
        assert_eq!(PageOffset(PAGES_PER_FILE).get_file_number(), 1);
        assert_eq!(PageOffset(PAGES_PER_FILE - 1).get_file_number(), 0);
        assert_eq!(PageOffset(PAGES_PER_FILE + 1).get_file_number(), 1);

        Ok(())
    }

    #[test]
    fn test_get_file_seek() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(PageOffset(0).get_file_seek(), 0);
        assert_eq!(PageOffset(PAGES_PER_FILE).get_file_seek(), 0);
        assert_eq!(
            PageOffset(PAGES_PER_FILE - 1).get_file_seek(),
            (PAGES_PER_FILE - 1) * PAGE_SIZE as usize
        );
        assert_eq!(
            PageOffset(PAGES_PER_FILE + 1).get_file_seek(),
            PAGE_SIZE as usize
        );
        assert_eq!(
            PageOffset(PAGES_PER_FILE + 2).get_file_seek(),
            2 * PAGE_SIZE as usize
        );

        Ok(())
    }

    #[test]
    fn test_is_same_file() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            PageOffset(0).is_same_file(&PageOffset(PAGES_PER_FILE)),
            false
        );
        assert_eq!(PageOffset(0).is_same_file(&PageOffset(0)), true);
        assert_eq!(
            PageOffset(0).is_same_file(&PageOffset(PAGES_PER_FILE - 1)),
            true
        );

        assert_eq!(
            PageOffset(PAGES_PER_FILE).is_same_file(&PageOffset(0)),
            false
        );
        assert_eq!(
            PageOffset(PAGES_PER_FILE - 1).is_same_file(&PageOffset(0)),
            true
        );

        Ok(())
    }
}
