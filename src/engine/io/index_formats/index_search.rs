use std::ops::{Bound, RangeBounds};
use thiserror::Error;

pub fn index_search_start<'a, K, R, T>(
    keys: &[K],
    pointers: &'a [T],
    range: R,
) -> Result<&'a T, IndexSearchError>
where
    K: PartialOrd,
    R: RangeBounds<K>,
    T: Clone,
{
    //Sanity checks
    if keys.is_empty() || pointers.is_empty() {
        return Err(IndexSearchError::Empty(keys.len(), pointers.len()));
    } else if keys.len() + 1 != pointers.len() {
        return Err(IndexSearchError::WrongCount(keys.len(), pointers.len()));
    }

    if let (Bound::Unbounded, Bound::Unbounded) = (range.start_bound(), range.end_bound()) {
        Ok(&pointers[0])
    } else if let Bound::Included(b) | Bound::Excluded(b) = range.start_bound() {
        for i in 0..keys.len() {
            if b <= &keys[i] {
                return Ok(&pointers[i]);
            }
        }
        Ok(&pointers[keys.len()])
    } else if let Bound::Included(b) | Bound::Excluded(b) = range.end_bound() {
        for i in (0..keys.len()).rev() {
            if &keys[i] <= b {
                return Ok(&pointers[i]);
            }
        }
        Ok(&pointers[0])
    } else {
        Err(IndexSearchError::UnreachableState())
    }
}

#[derive(Debug, Error)]
pub enum IndexSearchError {
    #[error("Either keys {0}, or pointers {1} are empty")]
    Empty(usize, usize),
    #[error("You should never get here")]
    UnreachableState(),
    #[error("Wrong count keys {0} must be one less than pointers {1}")]
    WrongCount(usize, usize),
}

#[cfg(test)]
mod tests {
    use std::ops::RangeFull;

    use super::*;

    #[test]
    fn test_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        //   0   1   2   3   4   5
        // a   b   c   d   e   f   g

        let keys = vec![0, 1, 2, 3, 4, 5];
        let pointers = vec!["a", "b", "c", "d", "e", "f", "g"];

        assert_eq!(index_search_start(&keys, &pointers, 1..3)?, &"b");
        assert_eq!(index_search_start(&keys, &pointers, 1..)?, &"b");
        assert_eq!(index_search_start(&keys, &pointers, ..4)?, &"e");
        assert_eq!(index_search_start(&keys, &pointers, RangeFull)?, &"a");
        assert_eq!(index_search_start(&keys, &pointers, 1..=3)?, &"b");
        assert_eq!(index_search_start(&keys, &pointers, ..=3)?, &"d");
        assert_eq!(index_search_start(&keys, &pointers, 6..9)?, &"g");

        //Edge cases
        assert_eq!(index_search_start(&keys, &pointers, ..0)?, &"a");

        Ok(())
    }
}
