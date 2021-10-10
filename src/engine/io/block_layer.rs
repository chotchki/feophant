/*

    Addressing:
        Uuid / Page Type / Page Offset

    Locking:
        Reading
        Writing

    Free Space:
        In Use
        Free

    I am most concerned about lost writes.

    Caching can move into the file layer, but locking stays out.

    File Manager handles I/O operations

    Free Space Manager guides what pages are usable

    Lock Cache Manager Handles locking


    Process:

        let page = get_page_for_read()
*/

//pub mod file_manager;
pub mod file_manager2;

pub mod file_operations;

pub mod free_space_manager;

pub mod lock_manager;

mod resource_formatter;
pub use resource_formatter::ResourceFormatter;
