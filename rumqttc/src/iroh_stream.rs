use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use iroh::{
    endpoint::{Connection, RecvStream, SendStream},
    Endpoint,
};
use n0_error::AnyError;
use tokio::io::{AsyncRead, AsyncWrite};

const ALPN: &[u8] = b"mqtt";

#[derive(Debug)]
pub struct IrohStream {
    _endpoint: Endpoint,
    _conn: Connection,
    send: SendStream,
    recv: RecvStream,
}

impl AsyncRead for IrohStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        AsyncRead::poll_read(Pin::new(&mut self.recv), cx, buf)
    }
}

impl AsyncWrite for IrohStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        AsyncWrite::poll_write(Pin::new(&mut self.send), cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        AsyncWrite::poll_flush(Pin::new(&mut self.send), cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        AsyncWrite::poll_shutdown(Pin::new(&mut self.send), cx)
    }
}

impl IrohStream {
    pub async fn connect(addr: &str) -> Result<Self, AnyError> {
        let remote_id: iroh::EndpointId = addr.parse().map_err(AnyError::from_stack)?;
        let endpoint = Endpoint::bind(iroh::endpoint::presets::N0).await?;
        let conn = endpoint.connect(remote_id, ALPN).await?;
        let (send, recv) = conn.open_bi().await.map_err(AnyError::from_std)?;
        let stream = IrohStream {
            send,
            recv,
            _conn: conn,
            _endpoint: endpoint,
        };
        Ok(stream)
    }
}
