use crate::{
    constants::{PAGES_PER_FILE, PAGE_SIZE},
    engine::io::ConstEncodedSize,
};
use std::{
    fmt,
    mem::size_of,
    num::TryFromIntError,
    ops::{Add, AddAssign, Mul},
};
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

    /// Gets the position of the free/visibility mask for an offset
    /// ```
    /// # use crate::feophantlib::engine::io::page_formats::PageOffset;
    /// let page = PageOffset(100);
    /// assert_eq!(page.get_bitmask_offset(), (PageOffset(0), 100));
    /// ```
    pub fn get_bitmask_offset(&self) -> (PageOffset, usize) {
        let offset = self.0 / (PAGE_SIZE as usize * 8);
        let inside_offset = self.0 % (PAGE_SIZE as usize * 8);
        (PageOffset(offset), inside_offset)
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

impl Add for PageOffset {
    type Output = PageOffset;

    fn add(self, rhs: Self) -> Self::Output {
        PageOffset(self.0 + rhs.0)
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

impl Mul for PageOffset {
    type Output = PageOffset;

    fn mul(self, rhs: Self) -> Self::Output {
        PageOffset(self.0 * rhs.0)
    }
}

#[derive(Debug, Error)]
pub enum PageOffsetError {
    #[error(transparent)]
    TryFromIntError(#[from] TryFromIntError),
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use uuid::Uuid;

    use super::*;

    #[test]
    fn test_add() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(PageOffset(1) + PageOffset(2), PageOffset(3));
        Ok(())
    }

    #[test]
    fn test_add_assign() -> Result<(), Box<dyn std::error::Error>> {
        let mut test = PageOffset(1);
        test += PageOffset(2);
        assert_eq!(test, PageOffset(3));
        Ok(())
    }

    #[test]
    fn test_mul() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(PageOffset(2) * PageOffset(3), PageOffset(6));
        Ok(())
    }

    #[test]
    fn test_calculate_page_offset() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(PageOffset::calculate_page_offset(0, 0), PageOffset(0));
        assert_eq!(
            PageOffset::calculate_page_offset(0, PAGE_SIZE as usize),
            PageOffset(1)
        );

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
        assert!(!PageOffset(0).is_same_file(&PageOffset(PAGES_PER_FILE)));
        assert!(PageOffset(0).is_same_file(&PageOffset(0)));
        assert!(PageOffset(0).is_same_file(&PageOffset(PAGES_PER_FILE - 1)));
        assert!(!PageOffset(PAGES_PER_FILE).is_same_file(&PageOffset(0)));
        assert!(PageOffset(PAGES_PER_FILE - 1).is_same_file(&PageOffset(0)));

        Ok(())
    }

    #[test]
    fn test_increment_and_hash_map() -> Result<(), Box<dyn std::error::Error>> {
        let test = PageOffset(0);
        assert_eq!(test.next(), PageOffset(1));

        let test_uuid = Uuid::new_v4();

        let mut resource_lookup: HashMap<Uuid, PageOffset> = HashMap::new();
        resource_lookup.insert(test_uuid, PageOffset(0));
        let test0 = resource_lookup.remove(&test_uuid).unwrap();
        resource_lookup.insert(test_uuid, test0.next());
        let test1 = resource_lookup.get(&test_uuid).unwrap();

        assert_eq!(*test1, PageOffset(1));
        Ok(())
    }
}
