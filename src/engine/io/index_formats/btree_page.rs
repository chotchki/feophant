use std::{fmt, mem::size_of, ops::Deref};

use crate::engine::io::ConstEncodedSize;

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub struct BTreePage(pub usize);

impl ConstEncodedSize for BTreePage {
    fn encoded_size() -> usize {
        size_of::<usize>()
    }
}

//ARGH this isn't working!
impl Deref for BTreePage {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for BTreePage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
