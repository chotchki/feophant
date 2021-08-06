//! Helper macro to make async unit testing easier
//! Many thanks to https://blog.x5ff.xyz/blog/async-tests-tokio-rust/

#[cfg(test)]
macro_rules! aw {
    ($e:expr) => {
        tokio_test::block_on($e)
    };
}
