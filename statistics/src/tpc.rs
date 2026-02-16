use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::AsyncWriteExt;
use serde::Serialize;
use tokio::runtime::Runtime;
use tokio::sync::broadcast::Sender;
use tracing::{error, info};
use tracing::log::warn;
use util::{Event, StatisticEvent, ThroughputEvent};

#[derive(Clone)]
struct EventState {
    sender: Sender<Event>,
    last_statistic: Arc<Mutex<StatisticEvent>>,
    last_tp: Arc<Mutex<ThroughputEvent>>
}

pub fn start(
    rt: &mut Runtime,
    tx: Sender<Event>,
    last_statistic: Arc<Mutex<StatisticEvent>>,
    last_tp: Arc<Mutex<ThroughputEvent>>,
) {
    let shared_state = EventState {
        sender: tx,
        last_statistic,
        last_tp,
    };

    rt.spawn(async move {
        // We bind to a port specifically for the raw TCP data stream
        let listener = TcpListener::bind("127.0.0.1:3132").await.unwrap();
        info!("TCP Event Server running on 127.0.0.1:3132");

        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    info!("New TCP connection: {}", addr);
                    let state = shared_state.clone();
                    tokio::spawn(async move {
                        handle_tcp_client(socket, state).await;
                    });
                }
                Err(e) => error!("TCP accept error: {}", e),
            }
        }
    });
}

async fn handle_tcp_client(mut socket: TcpStream, state: EventState) {
    let mut rx = state.sender.subscribe();

    // 1. Send Initial State (Statistics/TP)
    if let Err(e) = send_initial_sync(&mut socket, &state).await {
        error!("Failed to send initial TCP sync: {}", e);
        return;
    }

    // 2. Stream Loop
    loop {
        match rx.recv().await {
            Ok(event) => {
                // Filter logic (previously handled by path_str)
                // Since this is a single TCP port, we'll send all relevant dashboard events
                let should_send = match &event {
                    Event::Insert { .. } => true,
                    _ => true,
                };

                if should_send {
                    if let Err(_) = send_json_frame(&mut socket, &event).await {
                        break; // Client disconnected
                    }
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                warn!("TCP Client lagged by {} messages", n);
                continue;
            }
            Err(_) => break,
        }
    }
    info!("TCP Client disconnected");
}

/// Helper to send JSON followed by a newline (Line-delimited JSON)
async fn send_json_frame<T: Serialize>(socket: &mut TcpStream, data: &T) -> std::io::Result<()> {
    let mut serialized = serde_json::to_vec(data)?;
    serialized.push(b'\n'); // Delimiter
    socket.write_all(&serialized).await
}

async fn send_initial_sync(socket: &mut TcpStream, state: &EventState) -> std::io::Result<()> {
    let stats = (*state.last_statistic.lock().unwrap()).clone();
    send_json_frame(socket, &Event::Statistics(stats)).await?;

    let tp = (*state.last_tp.lock().unwrap()).clone();
    send_json_frame(socket, &Event::Throughput(tp)).await?;

    Ok(())
}