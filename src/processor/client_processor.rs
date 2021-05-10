use bytes::Bytes;
use hex_literal::hex;
use log;
use nom::{
    IResult,
    bytes::complete::tag,
    Err,
    InputIter,
    InputLength,
    InputTake,
    Needed,
    UnspecializedInput};
use std::iter::{Copied, Enumerate};
use std::slice::Iter;

use crate::codec::NetworkFrame;

pub struct ClientProcessor {}

impl ClientProcessor {
    pub fn process(&self, frame: NetworkFrame) -> Result<NetworkFrame, nom::Err<nom::error::Error<bytes::Bytes>>>{
        if frame.message_type == 0 {
            let is_ssl = self.is_ssl_request(frame.payload);
        }
    } 

    fn is_ssl_request(&self, input: Bytes) -> IResult<Bytes, Bytes> {
        tag(Bytes::from_static(&hex!("12 34 56 78")))(input)
    }
}

impl InputTake for Bytes {
    fn take(&self, count: usize) -> Self {
        self.slice(0..count)
    }
    fn take_split(&self, count: usize) -> (Self, Self) {
        let suffix = self.split_off(count);
        (suffix, *self)
    }
}

impl<'a> InputIter for &'a Bytes {
    type Item = u8;
    type Iter = Enumerate<Self::IterElem>;
    type IterElem = Copied<Iter<'a, u8>>;

    fn iter_indices(&self) -> Self::Iter {
      self.iter_elements().enumerate()
    }

    fn iter_elements(&self) -> Self::IterElem {
      self.iter().copied()
    }

    fn position<P>(&self, predicate: P) -> Option<usize>
    where
      P: Fn(Self::Item) -> bool,
    {
      self.iter().position(|b| predicate(*b))
    }

    fn slice_index(&self, count: usize) -> Result<usize, Needed> {
      if self.len() >= count {
        Ok(count)
      } else {
        Err(Needed::new(count - self.len()))
      }
    }
  }

  impl InputLength for Bytes {
    fn input_len(&self) -> usize {
        self.len()
    }
  }

  impl UnspecializedInput for Bytes {}