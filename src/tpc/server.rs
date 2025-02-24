use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token};
use schemas::message_generated::protocol::root_as_message;
use std::io::Read;
use std::net::SocketAddr;

const SERVER: Token = Token(0);

pub struct Server {
    port: u16,
    url: String,
}

impl Server {
    pub fn new(port: u16, url: String) -> Server {
        Server { port, url }
    }

    fn run(&self) {
        let addr: SocketAddr = format!("{}:{}", self.url, self.port).parse().unwrap();
        let mut listener = TcpListener::bind(addr).expect("Failed to bind server");

        let mut poll = Poll::new().unwrap();
        let mut events = Events::with_capacity(128);

        poll.registry()
            .register(&mut listener, SERVER, Interest::READABLE)
            .unwrap();

        println!("Server listening on {}", addr);

        loop {
            poll.poll(&mut events, None).unwrap();

            for event in &events {
                if event.token() == SERVER {
                    if let Ok((mut stream, _)) = listener.accept() {
                        println!("Client connected!");

                        let mut buffer = [0; 1024];
                        let bytes_read = stream.read(&mut buffer).unwrap();


                        let message = root_as_message(&buffer[..bytes_read]);
                        match message {
                            Ok(message) => {
                                println!("Received Message: content={}", message.data().unwrap());
                            }
                            Err(_) => {}
                        }

                    }
                }
            }
        }
    }
}