use feophantlib::engine::{io::IOManager, transactions::TransactionManager, Engine};

#[macro_export]
macro_rules! aw {
    ($e:expr) => {
        tokio_test::block_on($e)
    };
}

pub fn _create_engine() -> (TransactionManager, Engine) {
    let transaction_manager = TransactionManager::new();
    let engine = Engine::new(IOManager::new(), transaction_manager.clone());
    (transaction_manager, engine)
}
