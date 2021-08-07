use crate::engine::io::page_formats::PageOffset;
use bytes::Bytes;

#[derive(Debug, Clone)]
pub enum RequestType {
    Add(Bytes),
    Read(PageOffset),
    Update((PageOffset, Bytes)),
}
