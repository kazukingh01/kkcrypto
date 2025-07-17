use crate::models::{trade::{Trade, Side}, trade_candle::TradeCandle, market_type::MarketType};
use chrono::{DateTime, Duration, Timelike, Utc};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::error;

#[derive(Debug)]
struct TradeCandleBuffer {
    // Ask側データ (売り注文側の約定)
    ask_price: Option<f64>,  // 加重平均価格 (VWAP)
    ask_volume: f64,
    ask_count: i32,
    
    // Bid側データ (買い注文側の約定)
    bid_price: Option<f64>,  // 加重平均価格 (VWAP)
    bid_volume: f64,
    bid_count: i32,
    
    timestamp: DateTime<Utc>,
}

impl TradeCandleBuffer {
    fn new(timestamp: DateTime<Utc>) -> Self {
        Self {
            ask_price: None,
            ask_volume: 0.0,
            ask_count: 0,
            bid_price: None,
            bid_volume: 0.0,
            bid_count: 0,
            timestamp,
        }
    }

    fn update(&mut self, trade: &Trade) {
        match trade.side {
            Side::Sell => {
                // Bid側 (売り約定)
                // 逐次加重平均計算
                let new_total_volume = self.bid_volume + trade.quantity;
                if new_total_volume > 0.0 {
                    let current_vwap = self.bid_price.unwrap_or(0.0);
                    let new_vwap = (current_vwap * self.bid_volume + trade.price * trade.quantity) / new_total_volume;
                    self.bid_price = Some(new_vwap);
                }
                
                self.bid_volume = new_total_volume;
                self.bid_count += 1;
            }
            Side::Buy => {
                // Ask側 (買い約定)
                // 逐次加重平均計算
                let new_total_volume = self.ask_volume + trade.quantity;
                if new_total_volume > 0.0 {
                    let current_vwap = self.ask_price.unwrap_or(0.0);
                    let new_vwap = (current_vwap * self.ask_volume + trade.price * trade.quantity) / new_total_volume;
                    self.ask_price = Some(new_vwap);
                }
                
                self.ask_volume = new_total_volume;
                self.ask_count += 1;
            }
        }
    }

    fn to_trade_candle(&self, exchange: String, market_type: MarketType, symbol: String) -> TradeCandle {
        TradeCandle {
            id: uuid::Uuid::new_v4(),
            exchange,
            market_type,
            symbol,
            timestamp: self.timestamp,
            period_seconds: 1, // 1秒足
            ask_price: self.ask_price,
            ask_volume: self.ask_volume,
            ask_count: self.ask_count,
            bid_price: self.bid_price,
            bid_volume: self.bid_volume,
            bid_count: self.bid_count,
        }
    }
}

pub struct TradeCandleBuilder {
    trade_receiver: mpsc::Receiver<Trade>,
    candle_sender: mpsc::Sender<TradeCandle>,
    buffers: HashMap<(String, MarketType, String), TradeCandleBuffer>, // (exchange, market_type, symbol) -> buffer
}

impl TradeCandleBuilder {
    pub fn new(
        trade_receiver: mpsc::Receiver<Trade>,
        candle_sender: mpsc::Sender<TradeCandle>,
    ) -> Self {
        Self {
            trade_receiver,
            candle_sender,
            buffers: HashMap::new(),
        }
    }

    pub async fn start(mut self) {
        tracing::info!("TradeCandleBuilder started");
        let mut interval = interval(std::time::Duration::from_secs(1));
        
        loop {
            tokio::select! {
                Some(trade) = self.trade_receiver.recv() => {
                    self.process_trade(trade);
                }
                _ = interval.tick() => {
                    self.flush_candles().await;
                }
            }
        }
    }

    fn process_trade(&mut self, trade: Trade) {
        let current_second = trade.timestamp.with_nanosecond(0).unwrap();
        let key = (trade.exchange.clone(), trade.market_type.clone(), trade.symbol.clone());
        
        self.buffers
            .entry(key)
            .and_modify(|buffer| {
                if buffer.timestamp == current_second {
                    buffer.update(&trade);
                } else {
                    // 新しい秒になったので、新しいバッファを作成
                    *buffer = TradeCandleBuffer::new(current_second);
                    buffer.update(&trade);
                }
            })
            .or_insert_with(|| {
                let mut buffer = TradeCandleBuffer::new(current_second);
                buffer.update(&trade);
                buffer
            });
    }

    async fn flush_candles(&mut self) {
        let current_time = Utc::now().with_nanosecond(0).unwrap();
        let one_second_ago = current_time - Duration::seconds(1);
        
        for ((exchange, market_type, symbol), buffer) in &self.buffers {
            if buffer.timestamp <= one_second_ago {
                let candle = buffer.to_trade_candle(exchange.clone(), market_type.clone(), symbol.clone());
                
                tracing::info!("Sending candle: {} {} @ {}", exchange, symbol, buffer.timestamp.format("%H:%M:%S"));
                if let Err(e) = self.candle_sender.send(candle).await {
                    error!("Failed to send trade candle: {}", e);
                }
            }
        }
        
        // 古いバッファを削除
        self.buffers.retain(|_, buffer| buffer.timestamp > one_second_ago);
    }
}