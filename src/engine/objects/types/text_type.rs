use bytes::{Buf,Bytes};

use super::super::SqlType;
use super::super::SqlTypeError;

pub struct TextType {
    data: String
}

impl TextType {
    pub fn new(data: String) -> TextType {
        TextType {
            data
        }
    }

    pub fn get(&self) -> String {
        self.data.clone()
    }
}