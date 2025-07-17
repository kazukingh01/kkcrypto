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
struct HyperliquidSubscribe {
    method: String,
    subscription: HyperliquidSubscription,
}

#[derive(Debug, Serialize)]
struct HyperliquidSubscription {
    #[serde(rename = "type")]
    sub_type: String,
    coin: String,
}

#[derive(Debug, Deserialize)]
struct HyperliquidMessage {
    channel: String,
    data: Vec<HyperliquidTradeData>,
}

#[derive(Debug, Deserialize)]
struct HyperliquidTradeData {
    coin: String,
    side: String,
    px: String,
    sz: String,
    time: u64,
    hash: String,
}

pub struct HyperliquidClient {
    ws_stream: Option<WsStream>,
    trade_sender: mpsc::Sender<Trade>,
    trade_counter: AtomicU64,
    market_type: Option<MarketType>,
    raw_freq: u32,
}

impl HyperliquidClient {
    pub fn new(trade_sender: mpsc::Sender<Trade>, raw_freq: u32) -> Self {
        Self {
            ws_stream: None,
            trade_sender,
            trade_counter: AtomicU64::new(0),
            market_type: None,
            raw_freq,
        }
    }

    fn get_websocket_url(&self) -> &'static str {
        "wss://api.hyperliquid.xyz/ws"
    }

    async fn process_message(
        msg: Message,
        trade_sender: &mpsc::Sender<Trade>,
        _trade_counter: &AtomicU64,
        market_type: &MarketType,
    ) -> Result<()> {
        if let Message::Text(text) = msg {
            if let Ok(message) = serde_json::from_str::<HyperliquidMessage>(&text) {
                if message.channel == "trades" {
                    for trade_data in message.data {
                        let price = trade_data.px.parse::<f64>().unwrap_or(0.0);
                        let quantity = trade_data.sz.parse::<f64>().unwrap_or(0.0);
                        
                        let side = match trade_data.side.as_str() {
                            "A" => Side::Sell,  // Ask側の約定 = 売り
                            "B" => Side::Buy,   // Bid側の約定 = 買い
                            _ => Side::Buy,
                        };
                        
                        let timestamp = DateTime::from_timestamp_millis(trade_data.time as i64)
                            .unwrap_or_else(|| Utc::now());
                        
                        let trade = Trade::new(
                            "hyperliquid".to_string(),
                            market_type.clone(),
                            trade_data.coin,
                            trade_data.hash,
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
        Ok(())
    }
}

#[async_trait]
impl ExchangeClient for HyperliquidClient {
    async fn connect(&mut self, market_type: MarketType) -> Result<()> {
        let url = self.get_websocket_url();
        info!("Connecting to Hyperliquid {} WebSocket: {}", market_type.as_str().to_uppercase(), url);
        
        let (ws_stream, _) = connect_async(url).await?;
        self.ws_stream = Some(ws_stream);
        self.market_type = Some(market_type);
        
        info!("Connected to Hyperliquid {} WebSocket", self.market_type.as_ref().unwrap().as_str().to_uppercase());
        Ok(())
    }

    async fn subscribe_trades(&mut self, symbols: Vec<String>) -> Result<()> {
        if let Some(ws_stream) = &mut self.ws_stream {
            for symbol in symbols {
                let subscribe_msg = HyperliquidSubscribe {
                    method: "subscribe".to_string(),
                    subscription: HyperliquidSubscription {
                        sub_type: "trades".to_string(),
                        coin: symbol,
                    },
                };
                
                let msg = Message::Text(serde_json::to_string(&subscribe_msg)?);
                ws_stream.send(msg).await?;
            }
            
            info!("Subscribed to Hyperliquid {} trades", self.market_type.as_ref().unwrap().as_str().to_uppercase());
            
            // メッセージ処理ループ
            while let Some(msg) = ws_stream.next().await {
                match msg {
                    Ok(msg) => {
                        let count = self.trade_counter.fetch_add(1, Ordering::Relaxed);
                        // 1件目、(raw_freq+1)件目、(raw_freq*2+1)件目...を表示
                        if count % (self.raw_freq as u64) == 1 {
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
            info!("Disconnected from Hyperliquid {} WebSocket", 
                  self.market_type.as_ref().map_or("Unknown", |mt| mt.as_str()).to_uppercase());
        }
        Ok(())
    }
}