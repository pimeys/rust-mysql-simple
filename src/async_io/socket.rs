// Copyright (c) 2019 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use bytes::{Buf, BufMut};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_uds::UnixStream;

use std::{
    io,
    task::{Poll, Context},
    pin::Pin,
    path::Path,
};

/// Unix domain socket connection on unix, or named pipe connection on windows.
#[derive(Debug)]
pub struct Socket {
    inner: UnixStream,
}

impl Socket {
    /// Connects a new socket.
    pub async fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        Ok(Self {
            inner: UnixStream::connect(path).await?
        })
    }
}

impl AsyncRead for Socket {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for Socket {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}
