use std::collections::VecDeque;

use crate::{NetError, Result};
use bytes::BytesMut;
use rusty_mcrouter_protocol::{parse_reply, ProtocolError, Reply};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::{mpsc, oneshot},
};

use super::command::ClientCommand;
use super::config::ClientConfig;

pub(crate) struct ClientConnection {
    reader: OwnedReadHalf,
    writer: OwnedWriteHalf,
    rx: mpsc::Receiver<ClientCommand>,
    pending: VecDeque<oneshot::Sender<Result<Reply>>>,
    read_buf: BytesMut,
    write_buf: BytesMut,
}

impl ClientConnection {
    pub(crate) fn new(
        stream: TcpStream,
        rx: mpsc::Receiver<ClientCommand>,
        cfg: &ClientConfig,
    ) -> Self {
        let (reader, writer) = stream.into_split();

        Self {
            reader,
            writer,
            rx,
            pending: VecDeque::new(),
            read_buf: BytesMut::with_capacity(cfg.read_buf_initial_capacity),
            write_buf: BytesMut::new(),
        }
    }

    pub(crate) async fn run(mut self) {
        loop {
            tokio::select! {
                maybe_cmd = self.rx.recv() => {
                    match maybe_cmd {
                        Some(cmd) => {
                            if let Err(err) = self.write_one(cmd).await {
                                self.fail_all_pending(err);
                                return;
                            }
                        }
                        None => return,
                    }
                }
                res = self.reader.read_buf(&mut self.read_buf),
                    if !self.pending.is_empty() =>
                {
                    let n = match res {
                        Ok(n) => n,
                        Err(e) => {
                            self.fail_all_pending(NetError::Io(e));
                            return;
                        }
                    };
                    if n == 0 {
                        self.fail_all_pending(NetError::Io(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "backend closed connection",
                        )));
                        return;
                    }
                    if let Err(err) = self.deliver_replies() {
                        self.fail_all_pending(err);
                        return;
                    }
                }
            }
        }
    }

    // todo - make this a writev where we can write out multiple commands with one syscall
    async fn write_one(&mut self, cmd: ClientCommand) -> Result<()> {
        self.write_buf.clear();
        cmd.request.serialize_into(&mut self.write_buf);
        self.writer.write_all(&self.write_buf).await?;
        self.pending.push_back(cmd.reply_tx);
        Ok(())
    }

    fn deliver_replies(&mut self) -> Result<()> {
        while let Some(reply) = parse_reply(&mut self.read_buf)? {
            match self.pending.pop_front() {
                Some(tx) => {
                    let _ = tx.send(Ok(reply));
                }
                None => {
                    return Err(NetError::Protocol(ProtocolError::Malformed(
                        "unexpected reply with no pending request",
                    )));
                }
            }
        }
        Ok(())
    }

    fn fail_all_pending(&mut self, err: NetError) {
        for tx in self.pending.drain(..) {
            let _ = tx.send(Err(err.clone()));
        }
    }
}
