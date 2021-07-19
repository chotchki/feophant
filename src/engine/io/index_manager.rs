#[derive(Clone, Debug)]
pub struct IndexManager {
    io_manager: Arc<RwLock<IOManager>>,
}
