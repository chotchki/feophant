use tokio_util::codec::{Decoder,Encoder};
use bytes::{BytesMut, Buf, Bytes};
use hex_literal::hex;

use super::NetworkFrame;

pub struct PgCodec {}

const SSL_PAYLOAD: [u8; 4] = hex!("12 34 56 78");

impl Decoder for PgCodec {
    type Item = NetworkFrame;
    type Error = std::io::Error;

    fn decode(
        &mut self,
        src: &mut BytesMut
    ) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            // Not enough data to make a decision.
            return Ok(None);
        }

        //Read the first 4 bytes since it could be the special SSL message or a real message.
        let mut initial_bytes = [0u8; 4];
        initial_bytes.copy_from_slice(&src[..4]);

        //Handle all special messages
        if initial_bytes == SSL_PAYLOAD {
            src.advance(4);
            return Ok(Some(NetworkFrame::new(0, 4, Bytes::copy_from_slice(&initial_bytes))));
        }

        //Now we actually have to deal with real messages
        if src.len() < 5 {
            // Not enough data to make a decision.
            return Ok(None);
        }

        let mut message_bytes = [0u8; 1];
        message_bytes.copy_from_slice(&src[..1]);
        let message_type = u8::from_be(message_bytes[0]);

        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&src[1..5]);

        let length = u32::from_be_bytes(length_bytes) as u32;
        let length_size = u32::from_be_bytes(length_bytes) as usize;

        // TODO - Unsure how to stop DDOS when the protocol allows up to 2GB of data
        //          Would be great to know if the user is authenticated
        // Check that the length is not too large to avoid a denial of
        // service attack where the server runs out of memory.
        //if length > MAX {
        //    return Err(std::io::Error::new(
        //        std::io::ErrorKind::InvalidData,
        //        format!("Frame of length {} is too large.", length)
        //    ));
        //}

        if src.len() < 5 + length_size {
            // The full payload has not yet arrived.
            //
            // We reserve more space in the buffer. This is not strictly
            // necessary, but is a good idea performance-wise.
            src.reserve(5 + length_size - src.len());

            // We inform the Framed that we need more bytes to form the next
            // frame.
            return Ok(None);
        }

        // Use advance to modify src such that it no longer contains
        // this frame.
        let data = src[5..5 + length_size].to_vec();
        src.advance(5 + length_size);

        // Convert the data to a string, or fail if it is not valid utf-8.
        Ok(Some(NetworkFrame::new(message_type, length, Bytes::from(data))))
    }
}

impl Encoder<NetworkFrame> for PgCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: NetworkFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        if item.message_type == 0 {
            // Reserve space in the buffer.
            dst.reserve(item.payload.len());

            //Write to Buffer
            dst.extend_from_slice(&item.payload);
        } else {
            // Reserve space in the buffer.
            dst.reserve(5 + item.payload.len());

            //Enter the type
            dst.extend_from_slice(&[item.message_type][..]);

            // Convert the length into a byte array.
            // Protocol will always fit the u32
            let len_slice = u32::to_be_bytes(item.length);
            dst.extend_from_slice(&len_slice);
        }

        Ok(())
    }
}