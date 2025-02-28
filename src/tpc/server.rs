use std::collections::HashMap;
use crate::processing::station::Command;
use crossbeam::channel::{unbounded, Receiver, Sender};
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Registry, Token};
use std::io::{Error, Read};
use std::net::{SocketAddr, ToSocketAddrs};
use std::{io, thread};
use std::str::from_utf8;
use std::sync::Arc;
use mio::event::Event;
use tracing::{info};
use url::Url;
use crate::processing::Train;
use crate::util::Tx;

const SERVER: Token = Token(0);
const CLIENT: Token = Token(1);

pub struct Server {
    addr: SocketAddr,
}

impl Server {
    fn new(url: String) -> Server {
        let addr = Url::parse(&url).ok().unwrap();
        let host = addr.host_str().unwrap();
        let port = addr.port_or_known_default().unwrap();
        let addr = (host, port).to_socket_addrs().ok().unwrap().next().unwrap();
        Server { addr }
    }

    pub fn start(id: usize, url: String, rx: Receiver<Command>, outs: Vec<Tx<Train>>) -> Result<(), Error> {
        let server = Server::new(url);

        thread::spawn(move || match server.run(id, rx, outs) {
            Ok(_) => {}
            Err(err) => {
                tracing::error!("{}", format!("{}", err));
            }
        });

        Ok(())
    }

    fn run(&self, id: usize, rx: Receiver<Command>, outs: Vec<Tx<Train>>) -> Result<(), Error> {
        let mut server = TcpListener::bind(self.addr)?;
        let outs = Arc::new(outs);

        let mut poll = Poll::new()?;
        let mut events = Events::with_capacity(128);

        poll.registry()
            .register(&mut server, SERVER, Interest::READABLE)?;

        info!("TPC server listening on {}", self.addr);

        // Map of `Token` -> `TcpStream`.
        let mut connections = HashMap::new();
        // Unique token for each incoming connection.
        let mut unique_token = Token(SERVER.0 + 1);

        loop {
            if let Some(msg) = rx.try_recv().ok() {
                match msg {
                    Command::Stop(_) => {
                        return Ok(());
                    }
                    _ => {}
                }
            }

            if let Err(err) = poll.poll(&mut events, None) {
                if Server::interrupted(&err) {
                    continue;
                }
                return Err(err);
            }

            for event in events.iter() {
                match event.token() {
                    SERVER => loop {
                        // Received an event for the TCP server socket, which
                        // indicates we can accept an connection.
                        let (mut connection, address) = match server.accept() {
                            Ok((connection, address)) => (connection, address),
                            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                                // If we get a `WouldBlock` error we know our
                                // listener has no more incoming connections queued,
                                // so we can return to polling and wait for some
                                // more.
                                break;
                            }
                            Err(e) => {
                                // If it was any other kind of error, something went
                                // wrong and we terminate with an error.
                                return Err(e);
                            }
                        };

                        info!("Accepted connection from: {}", address);

                        let token = Server::next(&mut unique_token);
                        poll.registry().register(
                            &mut connection,
                            token,
                            Interest::READABLE.add(Interest::WRITABLE),
                        )?;

                        connections.insert(token, connection);
                    },
                    token => {
                        // Maybe received an event for a TCP connection.
                        let done = if let Some(connection) = connections.get_mut(&token) {
                            Server::handle_connection_event(id, outs.clone(), poll.registry(), connection, event)?
                        } else {
                            // Sporadic events happen, we can safely ignore them.
                            false
                        };
                        if done {
                            if let Some(mut connection) = connections.remove(&token) {
                                poll.registry().deregister(&mut connection)?;
                            }
                        }
                    }
                }
            }
        }
    }

    fn next(current: &mut Token) -> Token {
        let next = current.0;
        current.0 += 1;
        Token(next)
    }

    /// Returns `true` if the connection is done.
    fn handle_connection_event(
        id: usize,
        outs: Vec<Tx<Train>>,
        _registry: &Registry,
        connection: &mut TcpStream,
        event: &Event,
    ) -> io::Result<bool> {
        /*if event.is_writable() {
            // We can (maybe) write to the connection.
            match connection.write(DATA) {
                // We want to write the entire `DATA` buffer in a single go. If we
                // write less we'll return a short write error (same as
                // `io::Write::write_all` does).
                Ok(n) if n < DATA.len() => return Err(io::ErrorKind::WriteZero.into()),
                Ok(_) => {
                    // After we've written something we'll reregister the connection
                    // to only respond to readable events.
                    registry.reregister(connection, event.token(), Interest::READABLE)?
                }
                // Would block "errors" are the OS's way of saying that the
                // connection is not actually ready to perform this I/O operation.
                Err(ref err) if Server::would_block(err) => {}
                // Got interrupted (how rude!), we'll try again.
                Err(ref err) if Server::interrupted(err) => {
                    return Server::handle_connection_event(registry, connection, event)
                }
                // Other errors we'll consider fatal.
                Err(err) => return Err(err),
            }
        }*/

        if event.is_readable() {
            let mut connection_closed = false;
            let mut received_data = vec![0; 4096];
            let mut bytes_read = 0;
            // We can (maybe) read from the connection.
            loop {
                match connection.read(&mut received_data[bytes_read..]) {
                    Ok(0) => {
                        // Reading 0 bytes means the other side has closed the
                        // connection or is done writing, then so are we.
                        connection_closed = true;
                        break;
                    }
                    Ok(n) => {
                        bytes_read += n;
                        if bytes_read == received_data.len() {
                            received_data.resize(received_data.len() + 1024, 0);
                        }
                    }
                    // Would block "errors" are the OS's way of saying that the
                    // connection is not actually ready to perform this I/O operation.
                    Err(ref err) if Server::would_block(err) => break,
                    Err(ref err) if Server::interrupted(err) => continue,
                    // Other errors we'll consider fatal.
                    Err(err) => return Err(err),
                }
            }

            if bytes_read != 0 {
                let received_data = &received_data[..bytes_read];
                if let Ok(str_buf) = from_utf8(received_data) {
                    info!("Received data: {}", str_buf.trim_end());

                } else {
                    info!("Received (none UTF-8) data: {:?}", received_data);
                }
            }

            if connection_closed {
                info!("Connection closed");
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn would_block(err: &Error) -> bool {
        err.kind() == io::ErrorKind::WouldBlock
    }

    fn interrupted(err: &Error) -> bool {
        err.kind() == io::ErrorKind::Interrupted
    }
}
