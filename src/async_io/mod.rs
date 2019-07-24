// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures::{
    future::{poll_fn, select_ok},
    ready,
    sink::Sink,
    stream::{self, Stream as _},
};
use mysql_common::packets::RawPacket;
use tokio_codec::Framed;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_tcp::TcpStream;

use std::{
    fmt,
    future::Future,
    io,
    net::ToSocketAddrs,
    path::Path,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use crate::{
    async_io::{packet_codec::PacketCodec, socket::Socket},
    consts::MAX_PAYLOAD_LEN,
    error::*,
};

mod packet_codec;
mod socket;

pub trait MyFuture<T>: Future<Output = Result<T>> + Send + 'static {}

impl<T, U> MyFuture<T> for U where U: Future<Output = Result<T>> + Send + 'static {}

#[derive(Debug)]
pub enum Endpoint {
    Plain(TcpStream),
    Socket(Socket),
}

impl Endpoint {
    pub fn set_keepalive_ms(&self, ms: Option<u32>) -> Result<()> {
        let ms = ms.map(|val| Duration::from_millis(u64::from(val)));
        match *self {
            Endpoint::Plain(ref stream) => stream.set_keepalive(ms)?,
            Endpoint::Socket(_) => (/* inapplicable */),
        }
        Ok(())
    }

    pub fn set_tcp_nodelay(&self, val: bool) -> Result<()> {
        match *self {
            Endpoint::Plain(ref stream) => stream.set_nodelay(val)?,
            Endpoint::Socket(_) => (/* inapplicable */),
        }
        Ok(())
    }
}

impl From<TcpStream> for Endpoint {
    fn from(stream: TcpStream) -> Self {
        Endpoint::Plain(stream)
    }
}

impl From<Socket> for Endpoint {
    fn from(socket: Socket) -> Self {
        Endpoint::Socket(socket)
    }
}

impl AsyncRead for Endpoint {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Endpoint::Plain(ref mut tcp) => Pin::new(tcp).poll_read(cx, buf),
            Endpoint::Socket(ref mut sck) => Pin::new(sck).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Endpoint {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Endpoint::Plain(ref mut tcp) => Pin::new(tcp).poll_write(cx, buf),
            Endpoint::Socket(ref mut sck) => Pin::new(sck).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Endpoint::Plain(ref mut tcp) => Pin::new(tcp).poll_flush(cx),
            Endpoint::Socket(ref mut sck) => Pin::new(sck).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Endpoint::Plain(ref mut tcp) => Pin::new(tcp).poll_shutdown(cx),
            Endpoint::Socket(ref mut sck) => Pin::new(sck).poll_shutdown(cx),
        }
    }
}

/// Stream connected to MySql server.
pub struct Stream {
    closed: bool,
    codec: Framed<Endpoint, PacketCodec>,
}

impl fmt::Debug for Stream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Stream (endpoint={:?})", self.codec)
    }
}

impl Stream {
    fn new<T: Into<Endpoint>>(endpoint: T) -> Self {
        let endpoint = endpoint.into();

        Self {
            closed: false,
            codec: Framed::new(endpoint, packet_codec::PacketCodec::new()),
        }
    }

    pub async fn connect_tcp<S>(addr: S) -> io::Result<TcpStream>
    where
        S: ToSocketAddrs,
    {
        let addresses = addr.to_socket_addrs()?;
        let mut streams = Vec::new();

        for address in addresses {
            streams.push(TcpStream::connect(&address));
        }

        if !streams.is_empty() {
            Ok(select_ok(streams).await?.0)
        } else {
            let err = io::Error::new(
                io::ErrorKind::InvalidInput,
                "could not resolve to any address",
            );

            Err(err)
        }
    }

    pub async fn connect_socket<P: AsRef<Path>>(path: P) -> Result<Stream> {
        Ok(Stream::new(Socket::new(path).await?))
    }

    pub async fn write_packet(&mut self, data: Vec<u8>, seq_id: u8) -> Result<u8> {
        // at least one packet will be written
        let resulting_seq_id = seq_id.wrapping_add(1);

        // each new packet after 2²⁴−1 will add to the resulting sequence id
        let mut resulting_seq_id =
            resulting_seq_id.wrapping_add(((data.len() / MAX_PAYLOAD_LEN) % 256) as u8);

        // empty tail packet will also add to the resulting sequence id
        if !data.is_empty() && data.len() % MAX_PAYLOAD_LEN == 0 {
            resulting_seq_id = resulting_seq_id.wrapping_add(1);
        }

        Pin::new(&mut self.codec).start_send((RawPacket(data), seq_id))?;
        poll_fn(|mut cx| Pin::new(&mut self.codec).poll_flush(cx)).await?;
        Ok(resulting_seq_id)
    }
}

impl stream::Stream for Stream {
    type Item = Result<(RawPacket, u8)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if !self.closed {
            let state = ready!(Pin::new(&mut self.get_mut().codec).poll_next(cx));
            Poll::Ready(state.map(|s| s.map_err(Error::from)))
        } else {
            Poll::Ready(None)
        }
    }
}
