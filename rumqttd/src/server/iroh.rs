use crate::protocol::Protocol;
use crate::server::iroh::iroh_stream::IrohStream;
use crate::{IrohServerSettings, LinkType};
use flume::Sender;
use iroh::endpoint::presets;
use iroh::SecretKey;
use n0_error::AnyError;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::{error, field, info, Instrument};

use std::time::Duration;

use crate::router::Event;
use crate::ConnectionId;

use tokio::{task, time};

const ALPN: &[u8] = b"mqtt";

pub struct IrohServer<P> {
    config: IrohServerSettings,
    router_tx: Sender<(ConnectionId, Event)>,
    protocol: P,
    awaiting_will_handler: Arc<Mutex<HashMap<String, Sender<super::broker::AwaitingWill>>>>,
}

impl<P: Protocol + Clone + Send + 'static> IrohServer<P> {
    pub fn new(
        config: IrohServerSettings,
        router_tx: Sender<(ConnectionId, Event)>,
        protocol: P,
    ) -> IrohServer<P> {
        IrohServer {
            config,
            router_tx,
            protocol,
            awaiting_will_handler: Arc::new(Mutex::new(HashMap::default())),
        }
    }

    pub async fn start(&mut self, link_type: LinkType) -> Result<(), AnyError> {
        let secret_key = match self.config.secret_key.clone() {
            Some(s) => s.parse().map_err(AnyError::from_std)?,
            None => SecretKey::generate(),
        };
        let endpoint = iroh::Endpoint::builder(presets::N0)
            .secret_key(secret_key)
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await?;
        let delay = Duration::from_millis(self.config.next_connection_delay_ms);
        let mut count: usize = 0;

        let config = Arc::new(self.config.connections.clone());
        info!(
            config = self.config.name,
            endpoint_id = %endpoint.id(),
            "Listening for remote connections",
        );
        println!("iroh endpoint id: {}", endpoint.id());
        while let Some(incoming) = endpoint.accept().await {
            // Await new network connection.
            let conn = match incoming.accept() {
                Ok(incoming) => match incoming.await {
                    Ok(conn) => conn,
                    Err(err) => {
                        error!(error=?err, "Unable to accept socket.");
                        continue;
                    }
                },
                Err(err) => {
                    error!(error=?err, "Unable to accept socket.");
                    continue;
                }
            };
            // let (stream, addr) = match endpoint.accept().await {
            //     Ok((s, r)) => (s, r),
            //     Err(e) => {
            //         error!(error=?e, "Unable to accept socket.");
            //         continue;
            //     }
            // };

            let remote_id = conn.remote_id();
            let (network, tenant_id) = match IrohStream::accept(conn).await {
                Ok(s) => (s, remote_id.to_string()),
                Err(e) => {
                    error!(error=?e, "Iroh accept error");
                    continue;
                }
            };

            info!(
                name=?self.config.name, remote_id=%remote_id.fmt_short(), count, tenant=?tenant_id, "accept"
            );

            let config = config.clone();
            let router_tx = self.router_tx.clone();
            count += 1;

            let protocol = self.protocol.clone();
            match link_type {
                #[cfg(feature = "websocket")]
                LinkType::Websocket => {
                    unreachable!()
                }
                LinkType::Remote => task::spawn(
                    super::broker::remote(
                        config,
                        Some(tenant_id.clone()),
                        router_tx,
                        Box::new(network),
                        protocol,
                        self.awaiting_will_handler.clone(),
                    )
                    .instrument(tracing::error_span!(
                        "remote_link",
                        ?tenant_id,
                        client_id = field::Empty,
                        connection_id = field::Empty,
                    )),
                ),
            };

            time::sleep(delay).await;
        }
        Ok(())
    }
}

mod iroh_stream {
    use std::{
        io,
        pin::Pin,
        task::{Context, Poll},
    };

    use iroh::endpoint::{Connection, RecvStream, SendStream};
    use n0_error::AnyError;
    use tokio::io::{AsyncRead, AsyncWrite};

    #[derive(Debug)]
    pub struct IrohStream {
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

        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), io::Error>> {
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
        pub async fn accept(conn: Connection) -> Result<Self, AnyError> {
            let (send, recv) = conn.accept_bi().await.map_err(AnyError::from_std)?;
            let stream = IrohStream {
                send,
                recv,
                _conn: conn,
            };
            Ok(stream)
        }
    }
}
