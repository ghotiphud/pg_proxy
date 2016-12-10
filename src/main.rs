//! An extensible Postgres Proxy Server based on tokio-core
#![allow(unused_imports, unused_variables, unreachable_code)]

#[macro_use]
extern crate log;
extern crate env_logger;
extern crate dotenv;
#[macro_use]
extern crate futures;
#[macro_use]
extern crate tokio_core;
// extern crate tokio_service;

extern crate byteorder;
extern crate postgres_protocol;

use byteorder::{ByteOrder, BigEndian};
use dotenv::dotenv;

// use std::rc::Rc;
use std::env;
use std::net::SocketAddr;
use std::io::{Read, Write, Error};

use futures::{Future, Poll, Async, BoxFuture};
use futures::stream::Stream;
use tokio_core::io::{self, Io, ReadHalf, WriteHalf};
use tokio_core::net::{TcpStream, TcpListener};
use tokio_core::reactor::{Core};


// use postgres_protocol::message::{frontend, backend};
// use postgres_protocol::message::backend::{Message, ParseResult};


fn main() {
    env_logger::init().unwrap();
    dotenv().ok();

    // address that the proxy will bind to
    let bind_addr = env::args().nth(1).unwrap_or("127.0.0.1:5433".to_string());
    let bind_addr = bind_addr.parse::<SocketAddr>().unwrap();

    // address of the Postgres instance we are proxing for
    let postgres_addr = env::args().nth(2).unwrap_or("127.0.0.1:5432".to_string());
    let postgres_addr = postgres_addr.parse::<SocketAddr>().unwrap();

    // Create the tokio event loop that will drive this server
    let mut l = Core::new().unwrap();
    let handle = l.handle();

    // Create a TCP listener which will listen for incoming connections
    let listener = TcpListener::bind(&bind_addr, &handle).unwrap();
    println!("Listening on: {}", bind_addr);

    // for each incoming client connection
    let proxy = listener.incoming().for_each(move |(client_stream, addr)| {
        debug!("Client Stream Opened");
        // create a Postgres connection to serve requests
        let amts = TcpStream::connect(&postgres_addr, &handle).and_then(move |pg_stream| {
            debug!("Postgres Stream Opened");
            Pipe::new(client_stream, pg_stream)
        });

        let future = amts
            // .map(move |(pg_amt, client_amt)| {
            //     debug!("Connection finished");
            //     println!("wrote {} bytes to {} and {} bytes to {}",
            //              pg_amt,
            //              postgres_addr,
            //              client_amt,
            //              addr);
            // })
            .map_err(|e| {
                println!("error: {}", e);
            });

        // tell the tokio reactor to run the future
        handle.spawn(future);

        // everything is great!
        Ok(())
    });

    l.run(proxy).unwrap();
}

// /// Handlers return a variant of this enum to indicate how the proxy should handle the packet.
// pub enum Action {
//     /// drop the packet
//     Drop,
//     /// forward the packet unmodified
//     Forward,
//     /// forward a mutated packet
//     Mutate(Message),
//     /// respond to the packet without forwarding
//     Respond(Vec<Message>),
//     /// respond with an error packet
//     Error { code: u16, state: [u8; 5], msg: String },
// }

// pub trait MessageHandler {
//     fn handle_request(&mut self, m: &Message) -> Action;
//     fn handle_response(&mut self, m: &Message) -> Action;
// }

/// An enum representing Postgres backend message types
#[derive(Debug, PartialEq)]
pub enum MessageType {
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
    ParseComplete,
    PasswordMessage,
    PortalSuspended,
    ReadyForQuery,
    RowDescription,
    Terminate,
    Query,
    #[doc(hidden)]
    __ForExtensibility,
}

impl MessageType {
    pub fn get_type(buf: &[u8]) -> Option<(Self, usize)> {
        if buf.len() < 5 {
            return None;
        }

        let tag = buf[0];
        let len = BigEndian::read_u32(&buf[0..4]) as usize;

        debug!("tag: {}", tag as char);

        let msg_type = match tag {
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
            tag => {
                let mt = if len == 8 {
                    unimplemented!();
                    MessageType::SSLRequest
                } else {
                    MessageType::Startup
                };

                debug!("Type: {:?}, {:?}, {:?}", mt, len, buf.len());

                return Some((mt, len));
                //return None;
                // return Err(io::Error::new(io::ErrorKind::InvalidInput,
                //                           format!("unknown message tag `{}`", tag)));
            }
        };

        // 
        let len = BigEndian::read_u32(&buf[1..5]) as usize + 1;

        debug!("Type: {:?}, {:?}, {:?}", msg_type, len, buf.len());

        if len == buf.len() {
            Some((msg_type, len))
        } else {
            None
        }
    }
}

pub struct MessageStream {
    reader: ReadHalf<TcpStream>,
    read_buf: Vec<u8>,
    msg_buf: Vec<u8>,
}

impl MessageStream {
    fn new(reader: ReadHalf<TcpStream>) -> Self {
        MessageStream {
            reader: reader,
            read_buf: vec![0u8; 4096],
            msg_buf: Vec::with_capacity(4096),
        }
    }
}

impl Stream for MessageStream {
    type Item = (MessageType, Vec<u8>);
    type Error = std::io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // Read from the stream until we have a whole message
        loop {
            match self.reader.poll_read() {
                Async::Ready(_) => {
                    let n = try_nb!((&mut self.reader).read(&mut self.read_buf[..]));
                    // Signal that Stream is complete
                    // if n == 0 {
                    //     return Ok(Async::Ready(None));
                    // }
                    self.msg_buf.extend_from_slice(&self.read_buf[0..n]);

                    if let Some((msg_type, len)) = MessageType::get_type(&self.msg_buf[..]){
                        let msg_buf = self.msg_buf.drain(0..len).collect();
                        let msg = (msg_type, msg_buf);

                        return Ok(Async::Ready(Some(msg)));
                    }
                },
                _ => return Ok(Async::NotReady),
            }
        }
    }
}

pub struct Pipe {
    inner: BoxFuture<(),std::io::Error>,
}

impl Pipe {
    pub fn new(client_stream: TcpStream, pg_stream: TcpStream) -> Self {
        debug!("New Pipe: {:?} & {:?}", client_stream, pg_stream);

        let (reader, writer) = client_stream.split();
        let (pg_reader, mut pg_writer) = pg_stream.split();

        let msg_stream = MessageStream::new(reader)
            .for_each(move |(msg_type, msg_buf)| {
                
                if msg_type == MessageType::Query {
                    //let msg = Message::parse(&msg_buf).unwrap();

                    let query = std::ffi::CStr::from_bytes_with_nul(&msg_buf[5..]).unwrap()
                        .to_str().unwrap();

                    debug!("{}", query);
                }

                let n = pg_writer.write(&msg_buf[..]);

                Ok(())
            });

        let pg_response = io::copy(pg_reader, writer);

        let pipe = msg_stream.join(pg_response).map(|_| { });

        Pipe {
            inner: Box::new(pipe),
        }
    }
}

impl Future for Pipe {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.inner.poll()
    }
}