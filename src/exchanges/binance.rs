use crate::models::{trade::{Trade, Side}, market_type::MarketType, ExchangeClient};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use serde::Deserialize;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tracing::{error, info};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum BinanceMessage {
    // 複数シンボル用のストリーム形式
    Stream(BinanceStreamMessage),
    // 単一シンボル用の直接形式
    Direct(BinanceAggTradeData),
}

#[derive(Debug, Deserialize)]
struct BinanceStreamMessage {
    #[allow(dead_code)]
    stream: String,
    data: BinanceAggTradeData,
}

#[derive(Debug, Deserialize)]
struct BinanceAggTradeData {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "m")]
    is_buyer_maker: bool,
    #[serde(rename = "T")]
    timestamp: i64,
    #[serde(rename = "a")]
    trade_id: u64,
}

pub struct BinanceClient {
    ws_stream: Option<WsStream>,
    trade_sender: mpsc::Sender<Trade>,
    trade_counter: AtomicU64,
    market_type: Option<MarketType>,
    raw_freq: u32,
}

impl BinanceClient {
    pub fn new(trade_sender: mpsc::Sender<Trade>, raw_freq: u32) -> Self {
        Self {
            ws_stream: None,
            trade_sender,
            trade_counter: AtomicU64::new(0),
            market_type: None,
            raw_freq,
        }
    }

    fn build_websocket_url(&self, market_type: &MarketType, symbols: &[String]) -> String {
        let base_url = match market_type {
            MarketType::Spot => "wss://stream.binance.com:9443",
            MarketType::Linear => "wss://fstream.binance.com",
            MarketType::Inverse => "wss://dstream.binance.com",
        };
        
        let streams: Vec<String> = symbols
            .iter()
            .map(|s| format!("{}@aggTrade", s.to_lowercase()))
            .collect();
        
        if streams.len() == 1 {
            format!("{}/ws/{}", base_url, streams[0])
        } else {
            format!("{}/stream?streams={}", base_url, streams.join("/"))
        }
    }

    async fn process_message(
        msg: Message,
        trade_sender: &mpsc::Sender<Trade>,
        _trade_counter: &AtomicU64,
        market_type: &MarketType,
    ) -> Result<()> {
        if let Message::Text(text) = msg {
            if let Ok(message) = serde_json::from_str::<BinanceMessage>(&text) {
                let data = match message {
                    BinanceMessage::Stream(stream_msg) => stream_msg.data,
                    BinanceMessage::Direct(direct_data) => direct_data,
                };
                
                if data.event_type == "aggTrade" {
                    let price = data.price.parse::<f64>().unwrap_or(0.0);
                    let quantity = data.quantity.parse::<f64>().unwrap_or(0.0);
                    // Binanceでは is_buyer_maker が true なら買い、false なら売り
                    let side = if data.is_buyer_maker {
                        Side::Buy   // 買い手がメイカー = 買い約定 = Ask側
                    } else {
                        Side::Sell  // 買い手がテイカー = 売り約定 = Bid側
                    };
                    
                    let timestamp = DateTime::from_timestamp_millis(data.timestamp)
                        .unwrap_or_else(|| Utc::now());
                    
                    let trade = Trade::new(
                        "binance".to_string(),
                        market_type.clone(),
                        data.symbol,
                        data.trade_id.to_string(),
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
        Ok(())
    }
}

#[async_trait]
impl ExchangeClient for BinanceClient {
    async fn connect(&mut self, market_type: MarketType) -> Result<()> {
        // Note: URL will be built when subscribe_trades is called with symbols
        self.market_type = Some(market_type);
        Ok(())
    }

    async fn subscribe_trades(&mut self, symbols: Vec<String>) -> Result<()> {
        let market_type = self.market_type.as_ref().unwrap();
        let url = self.build_websocket_url(market_type, &symbols);
        info!("Connecting to Binance {} WebSocket: {}", market_type.as_str().to_uppercase(), url);
        
        let (ws_stream, _) = connect_async(url).await?;
        self.ws_stream = Some(ws_stream);
        
        info!("Connected and subscribed to Binance {} trades", market_type.as_str().to_uppercase());
        
        if let Some(ws_stream) = &mut self.ws_stream {
            // メッセージ処理ループ
            while let Some(msg) = ws_stream.next().await {
                match msg {
                    Ok(msg) => {
                        let count = self.trade_counter.fetch_add(1, Ordering::Relaxed);
                        // 1件目、(raw_freq+1)件目、(raw_freq*2+1)件目...を表示
                        if count % (self.raw_freq as u64) == 1 {
                            tracing::debug!("Raw message: {:?}", msg);
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
            info!("Disconnected from Binance {} WebSocket", 
                  self.market_type.as_ref().map_or("Unknown", |mt| mt.as_str()).to_uppercase());
        }
        Ok(())
    }
}