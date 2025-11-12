use env_logger::{Builder, Target};
use futures_util::{SinkExt, StreamExt};
use log::{LevelFilter, error, info, warn};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message};

/// Handles an individual WebSocket connection
async fn handle_connection(stream: TcpStream) {
    // Retrieve the client's IP address
    let addr = match stream.peer_addr() {
        Ok(a) => a,
        Err(e) => {
            error!("Failed to obtain client address: {}", e);
            return;
        }
    };
    info!("New connection from {}", addr);

    // Perform the WebSocket handshake
    let ws_stream = match accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            error!("WebSocket handshake failed for {}: {}", addr, e);
            return;
        }
    };

    info!("WebSocket connection established with {}", addr);

    // Split the stream into writer and reader
    let (mut write, mut read) = ws_stream.split();

    // Send a welcome message
    let welcome = "Welcome to the Echo Server!";
    if let Err(e) = write.send(Message::Text(welcome.into())).await {
        error!("Failed to send welcome message to {}: {}", addr, e);
        return;
    }
    info!("Welcome message sent to {}", addr);

    // Main loop: read and echo messages
    while let Some(msg_result) = read.next().await {
        match msg_result {
            Ok(Message::Text(text)) => {
                info!("Received from {}: '{}'", addr, text);
                if let Err(e) = write.send(Message::Text(text)).await {
                    error!("Error while sending to {}: {}", addr, e);
                    break;
                }
                info!("Message echoed back to {}", addr);
            }

            Ok(Message::Binary(data)) => {
                info!("Binary data received ({} bytes)", data.len());
                let _ = write.send(Message::Binary(data)).await;
            }

            Ok(Message::Ping(data)) => {
                info!("Ping received from {}", addr);
                let _ = write.send(Message::Pong(data)).await;
            }

            Ok(Message::Pong(_)) => {
                info!("Pong received from {}", addr);
            }

            Ok(Message::Close(frame)) => {
                info!("Close requested by client {:?} ({})", frame, addr);
                let _ = write.send(Message::Close(None)).await;
                break;
            }

            // New case added for version 0.24:
            Ok(Message::Frame(_)) => {
                warn!("Raw frame received from {}, ignored.", addr);
                continue;
            }

            Err(e) => {
                // Ignore non-standard opcodes (such as 7)
                if e.to_string().contains("invalid opcode") {
                    warn!("Invalid opcode received from {} – ignored", addr);
                    continue;
                }
                warn!("WebSocket error from {}: {}", addr, e);
                break;
            }
        }
    }

    info!("Connection closed with {}", addr);
}

/// Server entry point
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize the logger
    Builder::new()
        .target(Target::Stdout)
        .filter_level(LevelFilter::Info)
        .init();

    // Create the TCP listener
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    info!("WebSocket Echo Server listening on ws://127.0.0.1:8080");
    info!("Waiting for connections...");

    // Connection acceptance loop
    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(handle_connection(stream));
    }

    Ok(())
}