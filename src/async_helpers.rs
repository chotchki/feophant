//! Helper macro to make async unit testing easier
//! Many thanks to https://blog.x5ff.xyz/blog/async-tests-tokio-rust/

#[cfg(test)]
macro_rules! aw {
    ($e:expr) => {
        tokio_test::block_on($e)
    };
}

// Macro to be used when an await block might fail AND hang the test harness, didn't work right :(
//#[cfg(test)]
////macro_rules! awt {
//    ($e:expr) => {
//        tokio_test::block_on(tokio::time::timeout(std::time::Duration::new(10, 0), $e))
//    };
//}
