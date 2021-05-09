//! Implementation hints from here: https://docs.rs/tokio-util/0.6.6/tokio_util/codec/index.html

use tokio_util::codec::{Decoder,Encoder};
use bytes::{BytesMut, Buf, Bytes};
use hex_literal::hex;
use std::convert::TryFrom;

use super::NetworkFrame;

pub struct PgCodec {}

const SSL_PAYLOAD: [u8; 4] = hex!("12 34 56 78");

const MAX:u32 = u32::MAX;

impl Decoder for PgCodec {
    type Item = NetworkFrame;
    type Error = std::io::Error;

    fn decode(
        &mut self,
        src: &mut BytesMut
    ) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 5 {
            // Not enough data to make a decision.
            return Ok(None);
        }

        //Read the first byte
        let mut message_bytes = [0u8; 1];
        message_bytes.copy_from_slice(&src[..1]);
        let message_type = u8::from_be(message_bytes[0]);

        //If the message_type is 0, then it doesn't have a type and should just be seen as the length
        let prefix_len;
        if message_type == 0 {
            prefix_len = 4;
        } else {
            prefix_len = 5;
        }
        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&src[(prefix_len-4)..prefix_len]);

        let length = u32::from_be_bytes(length_bytes) as u32;

        if length < 4 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Frame length of {} is too small", length)
                ));
        }
        let length = length - 4;
        let length_size = u32::from_be_bytes(length_bytes) as usize - 4;


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

        if src.len() < prefix_len + length_size {
            // The full payload has not yet arrived.
            //
            // We reserve more space in the buffer. This is not strictly
            // necessary, but is a good idea performance-wise.
            src.reserve(prefix_len + length_size - src.len());

            // We inform the Framed that we need more bytes to form the next
            // frame.
            return Ok(None);
        }

        // Use advance to modify src such that it no longer contains
        // this frame.
        let data = src[prefix_len..prefix_len + length_size].to_vec();
        src.advance(prefix_len + length_size);

        // Convert the data to a string, or fail if it is not valid utf-8.
        Ok(Some(NetworkFrame::new(message_type, Bytes::from(data))))
    }
}

impl Encoder<NetworkFrame> for PgCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: NetworkFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        //Messages types of zero are special because they get written out raw. Probably should find a better way to do this
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
            let length = match u32::try_from(item.payload.len() + 4) {
                Ok(n) => n,
                Err(_) => return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Frame of length {} plus length header is too large.", item.payload.len())
                ))
            };

            let len_slice = u32::to_be_bytes(length);
            dst.extend_from_slice(&len_slice);

            dst.extend_from_slice(&item.payload);
        }
        Ok(())
    }
}