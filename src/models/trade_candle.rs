use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use super::market_type::MarketType;
use mongodb::bson::{doc, Document};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeCandle {
    pub id: Uuid,
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub period_seconds: i32,
    
    // Ask側データ (売り注文側の約定)
    pub ask_price: Option<f64>,  // 加重平均価格 (VWAP)
    pub ask_volume: f64,
    pub ask_count: i32,
    
    // Bid側データ (買い注文側の約定)
    pub bid_price: Option<f64>,  // 加重平均価格 (VWAP)
    pub bid_volume: f64,
    pub bid_count: i32,
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
            ask_price: None,
            ask_volume: 0.0,
            ask_count: 0,
            bid_price: None,
            bid_volume: 0.0,
            bid_count: 0,
        }
    }
    
    pub fn to_timeseries_document(&self) -> Document {
        use crate::utils::symbol_manager::SYMBOL_MANAGER;
        
        let ym = self.timestamp.format("%Y%m").to_string().parse::<i32>().unwrap_or(0);
        let unixtime = self.timestamp.timestamp();
        
        // symbol_idを取得
        let symbol_id = SYMBOL_MANAGER
            .get_symbol_id(&self.exchange, &self.symbol, self.market_type.as_str())
            .unwrap_or(0);
        
        doc! {
            "unixtime": mongodb::bson::DateTime::from_millis(unixtime * 1000),
            "metadata": {
                "ym": ym,
                "symbol": symbol_id
            },
            "ask_price": self.ask_price,
            "ask_volume": self.ask_volume,
            "ask_count": self.ask_count,
            "bid_price": self.bid_price,
            "bid_volume": self.bid_volume,
            "bid_count": self.bid_count
        }
    }
}