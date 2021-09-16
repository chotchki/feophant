//! Utility function to correctly split a branch and return a new sub set
// Split out to make testing easier
use thiserror::Error;

pub fn split_branch<T, U>(
    keys: &mut Vec<T>,
    pointers: &mut Vec<U>,
) -> Result<(T, Vec<T>, Vec<U>), SplitBranchError>
where
    T: Clone,
    U: Clone,
{
    if keys.len() + 1 != pointers.len() {
        return Err(SplitBranchError::KeysPointLen(keys.len(), pointers.len()));
    }

    if keys.len() < 2 {
        return Err(SplitBranchError::KeysTooShort(keys.len()));
    }

    let middle_index = keys.len() / 2;
    let middle = keys[middle_index].clone();

    let mut right_keys = vec![middle.clone(); keys.len() - middle_index - 1];
    right_keys.clone_from_slice(&keys[(middle_index + 1)..]);
    keys.truncate(middle_index);

    let mut right_pointers = vec![pointers[0].clone(); pointers.len() - middle_index - 1];
    right_pointers.clone_from_slice(&pointers[middle_index + 1..]);
    pointers.truncate(middle_index + 1);

    Ok((middle, right_keys, right_pointers))
}

#[derive(Debug, Error)]
pub enum SplitBranchError {
    #[error("Keys len {0}, must be one less than pointers len {1}")]
    KeysPointLen(usize, usize),
    #[error("Cannot split without at least 2 keys, have {0}")]
    KeysTooShort(usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_split_even() -> Result<(), Box<dyn std::error::Error>> {
        let mut keys = vec![0, 1, 2, 3, 4, 5];
        let mut pointers = vec!["a", "b", "c", "d", "e", "f", "g"];

        let left_keys = vec![0, 1, 2];
        let left_pointers = ["a", "b", "c", "d"];

        let middle = 3;
        let right_keys = vec![4, 5];
        let right_pointers = vec!["e", "f", "g"];

        let (out_middle, out_keys, out_pointers) = split_branch(&mut keys, &mut pointers)?;
        assert_eq!(keys, left_keys);
        assert_eq!(pointers, left_pointers);
        assert_eq!(out_middle, middle);
        assert_eq!(out_keys, right_keys);
        assert_eq!(out_pointers, right_pointers);

        Ok(())
    }

    #[test]
    fn test_simple_split_odd() -> Result<(), Box<dyn std::error::Error>> {
        let mut keys = vec![0, 1, 2, 3, 4];
        let mut pointers = vec!["a", "b", "c", "d", "e", "f"];

        let left_keys = vec![0, 1];
        let left_pointers = ["a", "b", "c"];

        let middle = 2;
        let right_keys = vec![3, 4];
        let right_pointers = vec!["d", "e", "f"];

        let (out_middle, out_keys, out_pointers) = split_branch(&mut keys, &mut pointers)?;
        assert_eq!(keys, left_keys);
        assert_eq!(pointers, left_pointers);
        assert_eq!(out_middle, middle);
        assert_eq!(out_keys, right_keys);
        assert_eq!(out_pointers, right_pointers);

        Ok(())
    }
}
