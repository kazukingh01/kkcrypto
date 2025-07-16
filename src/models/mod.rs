pub mod trade;
pub mod trade_candle;
pub mod market_type;

use async_trait::async_trait;
use anyhow::Result;
use market_type::MarketType;

#[derive(Debug, Clone)]
pub enum Exchange {
    Bybit,
    Binance,
}

#[async_trait]
pub trait ExchangeClient: Send + Sync {
    async fn connect(&mut self, market_type: MarketType) -> Result<()>;
    async fn subscribe_trades(&mut self, symbols: Vec<String>) -> Result<()>;
    async fn disconnect(&mut self) -> Result<()>;
}