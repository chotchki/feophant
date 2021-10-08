//! The system wide page size setting. This determines how much data is read and written at all times.
pub const PAGE_SIZE: u16 = 4096;

/// Max file size is 1GB. Be careful changing this setting on 32-bit platforms.
/// I have been careful to use usize in most places, as a result a variety of limits
/// will be lower on a 32bit platform.
pub const PAGES_PER_FILE: usize = 256;

/// Number of pages to hold in cache, each will consume PAGE_SIZE of memory
pub const MAX_PAGE_CACHE: usize = 128;

/// Linux seems to limit to 1024, macos 256, windows 512 but I'm staying low until
/// a benchmark proves I need to change it.
pub const MAX_FILE_HANDLE_COUNT: usize = 128;
