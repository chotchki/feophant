//! The system wide page size setting. This determines how much data is read and written at all times.
pub const PAGE_SIZE: u16 = 4096;

/// Max file size is 1GB. Be careful changing this setting on 32-bit platforms.
/// I have been careful to use usize in most places, as a result a variety of limits
/// will be lower on a 32bit platform.
pub const PAGES_PER_FILE: usize = 256;
