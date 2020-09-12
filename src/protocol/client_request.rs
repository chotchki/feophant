pub struct ClientRequest {
    message_type : u8,
    length: u32,
    payload: Vec<u8>
}