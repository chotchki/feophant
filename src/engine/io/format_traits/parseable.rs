//! Takes a byte buffer and constructs the object

use bytes::Buf;

pub trait Parseable<E> {
    type Output;

    fn parse(buffer: &mut impl Buf) -> Result<Self::Output, E>;
}
