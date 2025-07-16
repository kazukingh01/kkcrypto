use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use super::market_type::MarketType;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeCandle {
    pub id: Uuid,
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub period_seconds: i32,
    
    // Ask側データ (売り注文側の約定)
    pub ask_open: Option<f64>,
    pub ask_high: Option<f64>,
    pub ask_low: Option<f64>,
    pub ask_close: Option<f64>,
    pub ask_volume: f64,
    pub ask_count: i32,
    pub ask_vwap: Option<f64>,  // Volume Weighted Average Price
    
    // Bid側データ (買い注文側の約定)
    pub bid_open: Option<f64>,
    pub bid_high: Option<f64>,
    pub bid_low: Option<f64>,
    pub bid_close: Option<f64>,
    pub bid_volume: f64,
    pub bid_count: i32,
    pub bid_vwap: Option<f64>,  // Volume Weighted Average Price
}

impl TradeCandle {
    pub fn new(
        exchange: String,
        market_type: MarketType,
        symbol: String,
        timestamp: DateTime<Utc>,
        period_seconds: i32,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            exchange,
            market_type,
            symbol,
            timestamp,
            period_seconds,
            ask_open: None,
            ask_high: None,
            ask_low: None,
            ask_close: None,
            ask_volume: 0.0,
            ask_count: 0,
            ask_vwap: None,
            bid_open: None,
            bid_high: None,
            bid_low: None,
            bid_close: None,
            bid_volume: 0.0,
            bid_count: 0,
            bid_vwap: None,
        }
    }
}