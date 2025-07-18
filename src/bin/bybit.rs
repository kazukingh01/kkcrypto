use anyhow::Result;
use clap::Parser;
use kkcrypto::{
    db::Database,
    exchanges::bybit::BybitClient,
    models::{trade::Trade, trade_candle::TradeCandle, market_type::MarketType, ExchangeClient},
    utils::trade_candle_builder::TradeCandleBuilder,
};
use std::env;
use tokio::sync::mpsc;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(name = "bybit")]
#[command(about = "Collect real-time cryptocurrency trade data from Bybit", long_about = None)]
struct Args {
    /// Symbols to subscribe (comma-separated, e.g., BTCUSDT,ETHUSDT)
    #[arg(short, long, required = true)]
    symbols: String,

    /// Database URL (or use MONGODB_URL env var)
    #[arg(short, long)]
    database_url: Option<String>,

    /// Update database (if not set, only print data)
    #[arg(long)]
    update: bool,

    /// Use spot market
    #[arg(long)]
    spot: bool,

    /// Use linear futures market
    #[arg(long)]
    linear: bool,

    /// Use inverse futures market
    #[arg(long)]
    inverse: bool,

    /// Raw message print frequency (default: 100, minimum: 2)
    #[arg(long, default_value = "100", value_parser = clap::value_parser!(u32).range(2..))]
    raw_freq: u32,

    /// Timeframes to generate candles (comma-separated, e.g., 1m,5m,1h)
    #[arg(short = 't', long, default_value = "1m")]
    timeframes: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "kkcrypto=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Load .env file
    dotenv::dotenv().ok();

    // Parse command line arguments
    let args = Args::parse();
    
    // Determine market type
    let market_type = match (args.spot, args.linear, args.inverse) {
        (true, false, false) => MarketType::Spot,
        (false, true, false) => MarketType::Linear,
        (false, false, true) => MarketType::Inverse,
        (false, false, false) => {
            error!("Must specify one of --spot, --linear, or --inverse");
            std::process::exit(1);
        },
        _ => {
            error!("Can only specify one market type at a time");
            std::process::exit(1);
        }
    };
    
    // Parse symbols
    let symbols: Vec<String> = args
        .symbols
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    
    // Parse timeframes
    let timeframes: Vec<u32> = args
        .timeframes
        .split(',')
        .map(|s| {
            let trimmed = s.trim();
            // First try to parse as seconds
            if let Ok(seconds) = trimmed.parse::<u32>() {
                return seconds;
            }
            // Otherwise parse as time format
            match trimmed {
                "1s" => 1,
                "5s" => 5,
                "10s" => 10,
                "30s" => 30,
                "1m" => 60,
                "5m" => 300,
                "15m" => 900,
                "30m" => 1800,
                "1h" => 3600,
                "2h" => 7200,
                "4h" => 14400,
                "1d" => 86400,
                _ => {
                    error!("Invalid timeframe: {}. Use seconds (e.g., 1,5,60) or format (e.g., 1s,5s,1m,5m,1h)", trimmed);
                    std::process::exit(1);
                }
            }
        })
        .collect();
    
    info!("Starting Bybit {} trade collector with symbols: {:?}, timeframes: {:?}", 
          market_type.as_str().to_uppercase(), symbols, timeframes);

    // Create channels
    let (trade_tx, trade_rx) = mpsc::channel::<Trade>(1000);
    let (candle_tx, mut candle_rx) = mpsc::channel::<TradeCandle>(1000);

    // Start trade candle builder
    let candle_builder = TradeCandleBuilder::new(trade_rx, candle_tx, timeframes);
    tokio::spawn(async move {
        candle_builder.start().await;
    });

    // Handle database operations or print
    let db = if args.update {
        // Get database URL
        let database_url = args
            .database_url
            .or_else(|| env::var("MONGODB_URL").ok())
            .expect("MONGODB_URL must be set when using --update");

        // Initialize database with update flag
        Database::new(&database_url, true).await?
    } else {
        // Initialize dummy database for printing only
        Database::new("", false).await?
    };

    // Start database writer
    tokio::spawn(async move {
        while let Some(candle) = candle_rx.recv().await {
            println!(
                "[BYBIT-CANDLE] {} @ {} | Ask: Price:{} V:{:.4} Cnt:{} | Bid: Price:{} V:{:.4} Cnt:{}",
                candle.symbol, candle.timestamp.format("%H:%M:%S"),
                candle.ask_price.map_or("-".to_string(), |v| format!("{:.2}", v)),
                candle.ask_volume,
                candle.ask_count,
                candle.bid_price.map_or("-".to_string(), |v| format!("{:.2}", v)),
                candle.bid_volume,
                candle.bid_count
            );
            if let Err(e) = db.insert_trade_candle(&candle).await {
                error!("Failed to insert trade candle: {}", e);
            }
        }
    });

    // Start Bybit client
    let mut client = BybitClient::new(trade_tx, args.raw_freq);
    client.connect(market_type).await?;
    client.subscribe_trades(symbols).await?;

    Ok(())
}