use crate::models::{trade::{Trade, Side}, trade_candle::TradeCandle, market_type::MarketType};
use chrono::{DateTime, Duration, Utc};
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

    fn to_trade_candle(&self, exchange: String, market_type: MarketType, symbol: String, period_seconds: i32) -> TradeCandle {
        TradeCandle {
            id: uuid::Uuid::new_v4(),
            exchange,
            market_type,
            symbol,
            timestamp: self.timestamp,
            period_seconds,
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
    timeframes: Vec<u32>, // 時間枠のリスト (秒単位)
    buffers: HashMap<(String, MarketType, String, u32), TradeCandleBuffer>, // (exchange, market_type, symbol, timeframe) -> buffer
}

impl TradeCandleBuilder {
    pub fn new(
        trade_receiver: mpsc::Receiver<Trade>,
        candle_sender: mpsc::Sender<TradeCandle>,
        timeframes: Vec<u32>,
    ) -> Self {
        Self {
            trade_receiver,
            candle_sender,
            timeframes,
            buffers: HashMap::new(),
        }
    }

    pub async fn start(mut self) {
        tracing::info!("TradeCandleBuilder started with timeframes: {:?}", self.timeframes);
        
        // 各時間枠用のタスクを作成
        let (trigger_sender, mut trigger_receiver) = mpsc::channel::<u32>(100);
        
        // 各時間枠に対してタイマータスクを起動
        for &timeframe in &self.timeframes {
            let sender = trigger_sender.clone();
            tokio::spawn(async move {
                let mut interval = interval(std::time::Duration::from_secs(timeframe as u64));
                loop {
                    interval.tick().await;
                    if sender.send(timeframe).await.is_err() {
                        break;
                    }
                }
            });
        }
        
        loop {
            tokio::select! {
                Some(trade) = self.trade_receiver.recv() => {
                    self.process_trade(trade);
                }
                Some(timeframe) = trigger_receiver.recv() => {
                    self.flush_candles_for_timeframe(timeframe).await;
                }
            }
        }
    }

    fn process_trade(&mut self, trade: Trade) {
        // 各時間枠に対して処理
        for &timeframe in &self.timeframes {
            let timestamp = self.get_candle_timestamp(&trade.timestamp, timeframe);
            let key = (
                trade.exchange.clone(), 
                trade.market_type.clone(), 
                trade.symbol.clone(),
                timeframe
            );
            
            self.buffers
                .entry(key)
                .and_modify(|buffer| {
                    if buffer.timestamp == timestamp {
                        buffer.update(&trade);
                    } else {
                        // 新しい時間枠になったので、新しいバッファを作成
                        *buffer = TradeCandleBuffer::new(timestamp);
                        buffer.update(&trade);
                    }
                })
                .or_insert_with(|| {
                    let mut buffer = TradeCandleBuffer::new(timestamp);
                    buffer.update(&trade);
                    buffer
                });
        }
    }

    fn get_candle_timestamp(&self, timestamp: &DateTime<Utc>, timeframe_seconds: u32) -> DateTime<Utc> {
        let seconds_since_epoch = timestamp.timestamp();
        let candle_start = (seconds_since_epoch / timeframe_seconds as i64) * timeframe_seconds as i64;
        DateTime::from_timestamp(candle_start, 0).unwrap()
    }

    async fn flush_candles_for_timeframe(&mut self, timeframe: u32) {
        let current_time = Utc::now();
        let threshold = current_time - Duration::seconds(timeframe as i64);
        
        for ((exchange, market_type, symbol, tf), buffer) in &self.buffers {
            if *tf == timeframe && buffer.timestamp <= threshold {
                let candle = buffer.to_trade_candle(
                    exchange.clone(), 
                    market_type.clone(), 
                    symbol.clone(),
                    timeframe as i32
                );
                
                tracing::info!("Sending {}s candle: {} {} @ {}", 
                    timeframe, exchange, symbol, buffer.timestamp.format("%H:%M:%S"));
                if let Err(e) = self.candle_sender.send(candle).await {
                    error!("Failed to send trade candle: {}", e);
                }
            }
        }
        
        // 古いバッファを削除
        self.buffers.retain(|(_, _, _, tf), buffer| {
            *tf != timeframe || buffer.timestamp > threshold
        });
    }
}