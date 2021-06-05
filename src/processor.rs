//! This module covers taking a message and parsing it into a command object

mod client_processor;
pub use client_processor::ClientProcessor;

pub mod ssl_and_gssapi_parser;
pub mod startup_parser;
