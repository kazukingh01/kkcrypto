use crate::models::{trade::{Trade, Side}, trade_candle::TradeCandle, market_type::MarketType};
use chrono::{DateTime, Utc};
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
        // タイムスタンプを時間枠の開始時刻に正規化
        let seconds_since_epoch = self.timestamp.timestamp();
        let candle_start = (seconds_since_epoch / period_seconds as i64) * period_seconds as i64;
        let normalized_timestamp = DateTime::from_timestamp(candle_start, 0).unwrap();
        
        TradeCandle {
            id: uuid::Uuid::new_v4(),
            exchange,
            market_type,
            symbol,
            timestamp: normalized_timestamp,
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
                tracing::debug!("Timer task started for {}s timeframe", timeframe);
                loop {
                    interval.tick().await;
                    tracing::debug!("Timer tick for {}s timeframe", timeframe);
                    if sender.send(timeframe).await.is_err() {
                        tracing::error!("Timer task for {}s timeframe failed to send", timeframe);
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
                    tracing::debug!("Received timer trigger for {}s timeframe", timeframe);
                    self.flush_candles_for_timeframe(timeframe).await;
                }
            }
        }
    }

    fn process_trade(&mut self, trade: Trade) {
        // 各時間枠に対して処理
        for &timeframe in &self.timeframes {
            let key = (
                trade.exchange.clone(), 
                trade.market_type.clone(), 
                trade.symbol.clone(),
                timeframe
            );
            
            // バッファが存在しない場合は作成、存在する場合は更新のみ
            self.buffers
                .entry(key.clone())
                .and_modify(|buffer| {
                    buffer.update(&trade);
                })
                .or_insert_with(|| {
                    tracing::debug!("Creating new buffer for {} {} {}s", 
                        trade.exchange, trade.symbol, timeframe);
                    let mut buffer = TradeCandleBuffer::new(trade.timestamp);
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
        let candle_timestamp = self.get_candle_timestamp(&current_time, timeframe);
        
        tracing::debug!("Flushing {}s candles at {} (candle_timestamp: {})", 
            timeframe, 
            current_time.format("%H:%M:%S.%3f"),
            candle_timestamp.format("%H:%M:%S"));
        
        // 該当する時間枠のバッファを収集して送信
        let mut buffers_to_remove = Vec::new();
        let mut found_buffers = 0;
        let mut sent_candles = 0;
        
        for ((exchange, market_type, symbol, tf), buffer) in &self.buffers {
            if *tf == timeframe {
                found_buffers += 1;
                tracing::debug!("Found buffer for {}s: {} {} (ask_cnt:{}, bid_cnt:{})", 
                    timeframe, exchange, symbol, buffer.ask_count, buffer.bid_count);
                
                // バッファにデータがある場合のみ送信
                if buffer.ask_count > 0 || buffer.bid_count > 0 {
                    let candle = buffer.to_trade_candle(
                        exchange.clone(), 
                        market_type.clone(), 
                        symbol.clone(),
                        timeframe as i32
                    );
                    
                    tracing::debug!("Sending {}s candle: {} {} @ {} (ask_cnt:{}, bid_cnt:{})", 
                        timeframe, exchange, symbol, 
                        candle_timestamp.format("%H:%M:%S"),
                        buffer.ask_count, buffer.bid_count);
                    
                    if let Err(e) = self.candle_sender.send(candle).await {
                        error!("Failed to send trade candle: {}", e);
                    } else {
                        sent_candles += 1;
                    }
                } else {
                    tracing::debug!("Skipping empty buffer for {}s: {} {}", 
                        timeframe, exchange, symbol);
                }
                
                // このバッファを削除対象に追加
                buffers_to_remove.push((exchange.clone(), market_type.clone(), symbol.clone(), *tf));
            }
        }
        
        tracing::debug!("Flush {}s summary: found {} buffers, sent {} candles, removing {} buffers", 
            timeframe, found_buffers, sent_candles, buffers_to_remove.len());
        
        // 送信したバッファをクリア
        for key in &buffers_to_remove {
            self.buffers.remove(key);
        }
    }
}