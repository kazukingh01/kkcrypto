use sqlx::{postgres::PgPoolOptions, PgPool};
use anyhow::Result;

pub struct Database {
    pub pool: PgPool,
}

impl Database {
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await?;
        
        Ok(Self { pool })
    }

    pub async fn create_tables(&self) -> Result<()> {
        // Trade用1秒足データテーブル (ask/bid別)
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS trade_candles_1s (
                id UUID PRIMARY KEY,
                exchange VARCHAR(20) NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                period_seconds INTEGER NOT NULL,
                ask_open DOUBLE PRECISION,
                ask_high DOUBLE PRECISION,
                ask_low DOUBLE PRECISION,
                ask_close DOUBLE PRECISION,
                ask_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
                ask_count INTEGER NOT NULL DEFAULT 0,
                bid_open DOUBLE PRECISION,
                bid_high DOUBLE PRECISION,
                bid_low DOUBLE PRECISION,
                bid_close DOUBLE PRECISION,
                bid_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
                bid_count INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            "#
        )
        .execute(&self.pool)
        .await?;

        // インデックス
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_trade_candles_1s_exchange_symbol_timestamp 
            ON trade_candles_1s(exchange, symbol, timestamp DESC);
            "#
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn insert_trade_candle(&self, candle: &crate::models::trade_candle::TradeCandle) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO trade_candles_1s (
                id, exchange, symbol, timestamp, period_seconds,
                ask_open, ask_high, ask_low, ask_close, ask_volume, ask_count,
                bid_open, bid_high, bid_low, bid_close, bid_volume, bid_count
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
            "#
        )
        .bind(&candle.id)
        .bind(&candle.exchange)
        .bind(&candle.symbol)
        .bind(&candle.timestamp)
        .bind(candle.period_seconds)
        .bind(candle.ask_open)
        .bind(candle.ask_high)
        .bind(candle.ask_low)
        .bind(candle.ask_close)
        .bind(candle.ask_volume)
        .bind(candle.ask_count)
        .bind(candle.bid_open)
        .bind(candle.bid_high)
        .bind(candle.bid_low)
        .bind(candle.bid_close)
        .bind(candle.bid_volume)
        .bind(candle.bid_count)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}