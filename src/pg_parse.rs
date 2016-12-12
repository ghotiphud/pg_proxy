use byteorder::{ByteOrder, BigEndian};
use errors::*;

use postgres_protocol::message::{frontend, backend};
use postgres_protocol::message::backend::{ParseResult};

/// An enum representing Postgres frontend message types
#[derive(Debug, PartialEq)]
pub enum MessageType {
    ParseComplete,
    Startup,
    SSLRequest,
    Authentication,
    BackendKeyData,
    BindComplete,
    CloseComplete,
    CommandComplete,
    CopyData,
    CopyDone,
    CopyInResponse,
    CopyOutResponse,
    DataRow,
    EmptyQueryResponse,
    ErrorResponse,
    NoData,
    NoticeResponse,
    NotificationResponse,
    ParameterDescription,
    ParameterStatus,
    PasswordMessage,
    PortalSuspended,
    ReadyForQuery,
    RowDescription,
    Terminate,
    Query,

    NoTag,

    #[doc(hidden)]
    __ForExtensibility,
}

impl From<u8> for MessageType {
    fn from(val: u8) -> Self {
        match val {
            b'1' => MessageType::ParseComplete,
            b'2' => MessageType::BindComplete,
            b'3' => MessageType::CloseComplete,
            b'A' => MessageType::NotificationResponse,
            b'c' => MessageType::CopyDone,
            b'C' => MessageType::CommandComplete,
            b'd' => MessageType::CopyData,
            b'D' => MessageType::DataRow,
            b'E' => MessageType::ErrorResponse,
            b'G' => MessageType::CopyInResponse,
            b'H' => MessageType::CopyOutResponse,
            b'I' => MessageType::EmptyQueryResponse,
            b'K' => MessageType::BackendKeyData,
            b'n' => MessageType::NoData,
            b'N' => MessageType::NoticeResponse,
            b'R' => MessageType::Authentication,
            b's' => MessageType::PortalSuspended,
            b'S' => MessageType::ParameterStatus,
            b't' => MessageType::ParameterDescription,
            b'p' => MessageType::PasswordMessage,
            b'T' => MessageType::RowDescription,
            b'Z' => MessageType::ReadyForQuery,
            b'Q' => MessageType::Query,
            b'X' => MessageType::Terminate,
            _ => MessageType::NoTag
        }
    }
}

#[derive(Debug)]
pub struct MessagePacket {
    pub msg_type: MessageType,
    // TODO: not pub
    pub body: Vec<u8>
}

impl MessagePacket {
    fn new(msg_type: MessageType, body: Vec<u8>) -> Self {
        MessagePacket {
            msg_type: msg_type,
            body: body
        }
    }

    pub fn try_from(buf: &mut Vec<u8>) -> Result<Self> {
        if buf.len() < 5 {
            bail!("not enough data");
        }

        let tag = buf[0];
        let msg_type = tag.into();

        debug!("tag: {}", tag as char);

        let (msg_type, len) = match msg_type {
            MessageType::NoTag => {
                let len = BigEndian::read_u32(&buf[0..4]) as usize;

                if len == 8 {
                    unimplemented!();
                    (MessageType::SSLRequest, len)
                } else {
                    (MessageType::Startup, len)
                }
            },
            msg_type => {
                let len = BigEndian::read_u32(&buf[1..5]) as usize + 1;

                debug!("Type: {:?}, {:?}, {:?}", msg_type, len, buf.len());

                if len == buf.len() {
                    (msg_type, len)
                } else {
                    bail!("not fully loaded");
                }
            },
        };

        Ok(MessagePacket::new(msg_type, buf.drain(..len).collect()))
    }

    // pub fn to_message(&self) -> ::std::io::Result<ParseResult<frontend::borrowed::Message>> {
    //     frontend::borrowed::Message::parse(&self.body[..])
    // }

    pub fn write<W>(&self, writer: &mut W) -> ::std::io::Result<usize> 
        where W: ::std::io::Write {
        
        writer.write(&self.body[..])
    }
}