use crate::engine::io::page_formats::PageOffset;
use bytes::Bytes;

#[derive(Debug, Clone)]
pub enum ResponseType {
    Add(PageOffset),
    Read(Bytes),
    Update(()),
}
