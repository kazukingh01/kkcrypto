use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use super::market_type::MarketType;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub id: Uuid,
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: String,
    pub trade_id: String,
    pub price: f64,
    pub quantity: f64,
    pub side: Side,
    pub timestamp: DateTime<Utc>,
}

impl Trade {
    pub fn new(
        exchange: String,
        market_type: MarketType,
        symbol: String,
        trade_id: String,
        price: f64,
        quantity: f64,
        side: Side,
        timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            exchange,
            market_type,
            symbol,
            trade_id,
            price,
            quantity,
            side,
            timestamp,
        }
    }
}