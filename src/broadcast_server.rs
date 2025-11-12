use env_logger::{Builder, Target};
use futures_util::{SinkExt, StreamExt};
use log::{LevelFilter, error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PriceUpdate {
    symbol: String,
    price: f64,
    source: String,
    timestamp: i64,
}

/// Handles a connected client
async fn handle_client(stream: TcpStream, mut rx: broadcast::Receiver<PriceUpdate>) {
    let addr = match stream.peer_addr() {
        Ok(a) => a,
        Err(_) => return,
    };
    info!("New client: {}", addr);

    let ws_stream = match accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            error!("Handshake failed: {}", e);
            return;
        }
    };

    let (mut write, mut read) = ws_stream.split();

    // Welcome message
    let welcome = serde_json::json!({
        "type": "connected",
        "message": "Connected to the real-time feed"
    });
    let _ = write.send(Message::Text(welcome.to_string())).await;

    loop {
        tokio::select! {
            // Receive a price update and send it to the client
            Ok(update) = rx.recv() => {
                let json = match serde_json::to_string(&update) {
                    Ok(j) => j,
                    Err(e) => {
                        warn!("Serialization error: {}", e);
                        continue;
                    }
                };

                if write.send(Message::Text(json)).await.is_err() {
                    info!("Client disconnected: {}", addr);
                    break;
                }
            }

            // Handle incoming messages from the client (ping, etc.)
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => {
                        info!("Client closed: {}", addr);
                        break;
                    }
                    Some(Ok(Message::Ping(d))) => {
                        let _ = write.send(Message::Pong(d)).await;
                    }
                    Some(Ok(Message::Text(t))) => {
                        info!("Client {} said: {}", addr, t);
                    }
                    Some(Err(e)) => {
                        warn!("WebSocket error from {}: {}", addr, e);
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    info!("Connection ended for {}", addr);
}

/// Simulates random prices
async fn price_simulator(tx: broadcast::Sender<PriceUpdate>) {
    use rand::Rng;
    use tokio::time::{sleep, Duration};

    let symbols = vec!["AAPL", "GOOGL", "MSFT", "AMZN"];
    let sources = vec!["alpha_vantage", "yahoo_finance"];

    loop {
        sleep(Duration::from_secs(2)).await;

        let mut rng = rand::rng();
        let update = PriceUpdate {
            symbol: symbols[rng.random_range(0..symbols.len())].to_string(),
            price: rng.random_range(100.0..500.0),
            source: sources[rng.random_range(0..sources.len())].to_string(),
            timestamp: chrono::Utc::now().timestamp(),
        };

        info!("Broadcasting: {} = {:.2} ({})", update.symbol, update.price, update.source);
        let _ = tx.send(update);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    Builder::new()
        .target(Target::Stdout)
        .filter_level(LevelFilter::Info)
        .init();

    // Broadcast channel (max 100 messages in buffer)
    let (tx, _rx) = broadcast::channel::<PriceUpdate>(100);

    // Simulation task
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        price_simulator(tx_clone).await;
    });

    // WebSocket server
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    info!("Broadcast server listening on ws://127.0.0.1:8080");

    while let Ok((stream, _)) = listener.accept().await {
        let rx = tx.subscribe();
        tokio::spawn(handle_client(stream, rx));
    }

    Ok(())
}