use crate::models::{trade::{Trade, Side}, market_type::MarketType, ExchangeClient};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tracing::{error, info};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Serialize)]
struct BybitSubscribe {
    op: String,
    args: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct BybitResponse {
    topic: Option<String>,
    data: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct BybitTradeData {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "v")]
    quantity: String,
    #[serde(rename = "S")]
    side: String,
    #[serde(rename = "T")]
    timestamp: i64,
    #[serde(rename = "i")]
    trade_id: String,
}

pub struct BybitClient {
    ws_stream: Option<WsStream>,
    trade_sender: mpsc::Sender<Trade>,
    trade_counter: AtomicU64,
    market_type: Option<MarketType>,
}

impl BybitClient {
    pub fn new(trade_sender: mpsc::Sender<Trade>) -> Self {
        Self {
            ws_stream: None,
            trade_sender,
            trade_counter: AtomicU64::new(0),
            market_type: None,
        }
    }

    fn get_websocket_url(&self, market_type: &MarketType) -> &'static str {
        match market_type {
            MarketType::Spot => "wss://stream.bybit.com/v5/public/spot",
            MarketType::Linear => "wss://stream.bybit.com/v5/public/linear",
            MarketType::Inverse => "wss://stream.bybit.com/v5/public/inverse",
        }
    }

    async fn process_message(
        msg: Message,
        trade_sender: &mpsc::Sender<Trade>,
        trade_counter: &AtomicU64,
        market_type: &MarketType,
    ) -> Result<()> {
        if let Message::Text(text) = msg {
            let response: BybitResponse = serde_json::from_str(&text)?;
            
            if let Some(topic) = &response.topic {
                if topic.starts_with("publicTrade.") {
                    if let Some(data) = response.data {
                        if let Ok(trades) = serde_json::from_value::<Vec<BybitTradeData>>(data) {
                            for trade_data in trades {
                                let _count = trade_counter.fetch_add(1, Ordering::Relaxed);
                                
                                let price = trade_data.price.parse::<f64>().unwrap_or(0.0);
                                let quantity = trade_data.quantity.parse::<f64>().unwrap_or(0.0);
                                let side = match trade_data.side.as_str() {
                                    "Buy" => Side::Buy,
                                    "Sell" => Side::Sell,
                                    _ => Side::Buy, // デフォルト
                                };
                                
                                let timestamp = DateTime::from_timestamp_millis(trade_data.timestamp)
                                    .unwrap_or_else(|| Utc::now());
                                
                                let trade = Trade::new(
                                    "bybit".to_string(),
                                    market_type.clone(),
                                    trade_data.symbol,
                                    trade_data.trade_id,
                                    price,
                                    quantity,
                                    side,
                                    timestamp,
                                );
                                
                                
                                if let Err(e) = trade_sender.send(trade).await {
                                    error!("Failed to send trade: {}", e);
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ExchangeClient for BybitClient {
    async fn connect(&mut self, market_type: MarketType) -> Result<()> {
        let url = self.get_websocket_url(&market_type);
        info!("Connecting to Bybit {} WebSocket: {}", market_type.as_str().to_uppercase(), url);
        
        let (ws_stream, _) = connect_async(url).await?;
        self.ws_stream = Some(ws_stream);
        self.market_type = Some(market_type);
        
        info!("Connected to Bybit {} WebSocket", self.market_type.as_ref().unwrap().as_str().to_uppercase());
        Ok(())
    }

    async fn subscribe_trades(&mut self, symbols: Vec<String>) -> Result<()> {
        if let Some(ws_stream) = &mut self.ws_stream {
            let args: Vec<String> = symbols
                .into_iter()
                .map(|symbol| format!("publicTrade.{}", symbol))
                .collect();
            
            let subscribe_msg = BybitSubscribe {
                op: "subscribe".to_string(),
                args,
            };
            
            let msg = Message::Text(serde_json::to_string(&subscribe_msg)?);
            ws_stream.send(msg).await?;
            
            info!("Subscribed to Bybit trades");
            
            // メッセージ処理ループ
            while let Some(msg) = ws_stream.next().await {
                match msg {
                    Ok(msg) => {
                        let count = self.trade_counter.fetch_add(1, Ordering::Relaxed);
                        // 1件目、101件目、201件目...を表示
                        if count % 100 == 1 {
                            println!("Raw message: {:?}", msg);
                        }
                        // カウンターを定期的にリセット (100万件毎)
                        if count >= 1_000_000 {
                            self.trade_counter.store(0, Ordering::Relaxed);
                        }
                        if let Err(e) = Self::process_message(msg, &self.trade_sender, &self.trade_counter, self.market_type.as_ref().unwrap()).await {
                            error!("Error processing message: {}", e);
                        }
                    }
                    Err(e) => {
                        error!("WebSocket error: {}", e);
                        break;
                    }
                }
            }
        }
        
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        if let Some(mut ws_stream) = self.ws_stream.take() {
            ws_stream.close(None).await?;
            info!("Disconnected from Bybit WebSocket");
        }
        Ok(())
    }
}