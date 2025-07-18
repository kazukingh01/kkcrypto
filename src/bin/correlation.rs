use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use clap::Parser;
use mongodb::{
    bson::{doc, Document},
    Client,
};
use polars::prelude::*;
use std::collections::HashMap;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(name = "correlation")]
#[command(about = "Real-time correlation calculator for cryptocurrency data")]
struct Args {
    /// MongoDB URL (or use MONGODB_URL env var)
    #[arg(short, long)]
    database_url: Option<String>,

    /// Correlation window in minutes (default: 30)
    #[arg(short = 'w', long, default_value = "30")]
    window_minutes: u32,

    /// Minimum data points required for correlation (default: 300)
    #[arg(short = 'm', long, default_value = "300")]
    min_data_points: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("[STARTUP] Starting correlation program...");
    
    // Load .env file
    dotenv::dotenv().ok();
    println!("[STARTUP] Loaded .env file");
    
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "correlation=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
    println!("[STARTUP] Initialized tracing");

    let args = Args::parse();
    println!("[STARTUP] Parsed args: window_minutes={}, min_data_points={}", args.window_minutes, args.min_data_points);

    // Get database URL
    println!("[STARTUP] Getting database URL...");
    let database_url = args
        .database_url
        .or_else(|| std::env::var("MONGODB_URL").ok())
        .expect("MONGODB_URL must be set");
    println!("[STARTUP] Database URL: {}", database_url.replace(|c: char| c.is_alphanumeric() || c == '@' || c == '.' || c == ':', "*"));

    // Connect to MongoDB
    println!("[STARTUP] Connecting to MongoDB...");
    let client = Client::with_uri_str(&database_url).await?;
    println!("[STARTUP] Connected to MongoDB client");
    let db = client.database("trade");
    println!("[STARTUP] Selected database: trade");
    let collection = db.collection::<Document>("candles_5s");
    println!("[STARTUP] Selected collection: candles_5s");

    info!("Connected to MongoDB");

    // Create correlation calculator
    let mut calculator = CorrelationCalculator::new(
        collection.clone(),
        args.window_minutes,
        args.min_data_points,
    );

    // Load initial data
    info!("Loading initial {} minutes of data...", args.window_minutes);
    calculator.load_initial_data().await?;

    // Use polling approach
    info!("Starting polling mode for candles_5s collection (interval: 5 seconds)...");
    let mut last_processed_time = Utc::now();
    
    loop {
        // Wait for 5 seconds
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        
        let current_time = Utc::now();
        
        // Query for new data since last processed time
        let filter = doc! {
            "unixtime": { 
                "$gt": last_processed_time.timestamp_millis(),
                "$lte": current_time.timestamp_millis()
            }
        };
        
        info!("Polling candles_5s for new data from {} to {}", 
            last_processed_time.format("%H:%M:%S"), 
            current_time.format("%H:%M:%S"));
        
        let mut cursor = collection.find(filter).await?;
        let mut processed_count = 0;
        
        while cursor.advance().await? {
            let raw_doc = cursor.current();
            let doc: Document = raw_doc.try_into()?;
            
            // Print raw data for debugging (only first few)
            if processed_count < 3 {
                println!("=== RAW CANDLE DATA ===");
                println!("{}", serde_json::to_string_pretty(&doc).unwrap_or_else(|e| format!("JSON Error: {}", e)));
                println!("========================");
            }
            
            if let Err(e) = calculator.process_new_candle(doc).await {
                error!("Error processing candle: {}", e);
            } else {
                processed_count += 1;
            }
        }
        
        if processed_count > 0 {
            info!("Processed {} new candles", processed_count);
        } else {
            info!("No new data found");
        }
        
        last_processed_time = current_time;
    }
}

struct CorrelationCalculator {
    collection: mongodb::Collection<Document>,
    window_minutes: u32,
    min_data_points: usize,
    data_cache: HashMap<i32, DataFrame>, // symbol_id -> DataFrame
}

impl CorrelationCalculator {
    fn new(
        collection: mongodb::Collection<Document>,
        window_minutes: u32,
        min_data_points: usize,
    ) -> Self {
        Self {
            collection,
            window_minutes,
            min_data_points,
            data_cache: HashMap::new(),
        }
    }

    async fn load_initial_data(&mut self) -> Result<()> {
        let start_time = Utc::now() - Duration::minutes(self.window_minutes as i64);
        let start_time_ms = start_time.timestamp_millis();
        
        info!("Loading data from {} ({}ms)", start_time.format("%Y-%m-%d %H:%M:%S"), start_time_ms);
        
        // Query for all data in the window
        let filter = doc! {
            "unixtime": { "$gte": start_time_ms }
        };
        
        info!("Executing find query with filter: {:?}", filter);
        let mut cursor = self.collection.find(filter).await?;
        let mut data_by_symbol: HashMap<i32, Vec<(DateTime<Utc>, f64)>> = HashMap::new();
        let mut total_docs = 0;
        
        // Collect data by symbol
        while cursor.advance().await? {
            let raw_doc = cursor.current();
            let doc: Document = raw_doc.try_into()?;
            
            // Print first few documents for debugging
            if data_by_symbol.len() < 3 {
                println!("=== RAW INITIAL DATA ===");
                println!("{}", serde_json::to_string_pretty(&doc).unwrap_or_else(|e| format!("JSON Error: {}", e)));
                println!("========================");
            }
            
            if let (Ok(symbol_id), Ok(timestamp_ms)) = (
                doc.get_document("metadata")?.get_i32("symbol"),
                doc.get_i64("unixtime"),
            ) {
                // Get ask and bid prices
                let ask_price = doc.get_f64("ask_price").ok();
                let bid_price = doc.get_f64("bid_price").ok();
                
                // Calculate average price (mid price)
                let price = match (ask_price, bid_price) {
                    (Some(ask), Some(bid)) => (ask + bid) / 2.0,
                    (Some(ask), None) => ask,
                    (None, Some(bid)) => bid,
                    (None, None) => continue, // Skip if both are null
                };
                
                let timestamp = DateTime::from_timestamp_millis(timestamp_ms).unwrap();
                data_by_symbol
                    .entry(symbol_id)
                    .or_insert_with(Vec::new)
                    .push((timestamp, price));
                total_docs += 1;
            }
        }
        
        info!("Loaded {} documents for {} symbols", total_docs, data_by_symbol.len());
        if data_by_symbol.is_empty() {
            println!("WARNING: No data found in the last {} minutes!", self.window_minutes);
        }
        
        // Convert to DataFrames with filled missing values
        for (symbol_id, data) in data_by_symbol {
            if let Ok(df) = self.create_filled_dataframe(data, start_time) {
                self.data_cache.insert(symbol_id, df);
            }
        }
        
        Ok(())
    }

    fn create_filled_dataframe(
        &self,
        data: Vec<(DateTime<Utc>, f64)>,
        start_time: DateTime<Utc>,
    ) -> Result<DataFrame> {
        // Create time series with 5-second intervals
        let end_time = Utc::now();
        let mut timestamps = vec![];
        let mut current = start_time;
        
        while current <= end_time {
            timestamps.push(current.timestamp_millis());
            current = current + Duration::seconds(5);
        }
        
        // Create DataFrame with all timestamps
        let timestamp_series = Series::new("timestamp", timestamps.clone());
        let base_df = DataFrame::new(vec![timestamp_series])?;
        
        // Create DataFrame from actual data
        let data_timestamps: Vec<i64> = data.iter().map(|(t, _)| t.timestamp_millis()).collect();
        let data_prices: Vec<f64> = data.iter().map(|(_, p)| *p).collect();
        
        let data_df = DataFrame::new(vec![
            Series::new("timestamp", data_timestamps),
            Series::new("price", data_prices),
        ])?;
        
        // Join and forward fill missing values
        let joined = base_df
            .join(
                &data_df,
                ["timestamp"],
                ["timestamp"],
                JoinArgs::new(JoinType::Left),
            )?;
        
        // Forward fill missing values
        let filled = joined
            .lazy()
            .with_column(col("price").forward_fill(None))
            .collect()?;
        
        Ok(filled)
    }

    async fn process_new_candle(&mut self, candle: Document) -> Result<()> {
        // Extract candle data
        let symbol_id = candle.get_document("metadata")?.get_i32("symbol")?;
        let timestamp_ms = candle.get_i64("unixtime")?;
        let timestamp = DateTime::from_timestamp_millis(timestamp_ms).unwrap();
        
        // Get ask and bid prices
        let ask_price = candle.get_f64("ask_price").ok();
        let bid_price = candle.get_f64("bid_price").ok();
        
        // Calculate average price (mid price)
        let price = match (ask_price, bid_price) {
            (Some(ask), Some(bid)) => (ask + bid) / 2.0,
            (Some(ask), None) => ask,
            (None, Some(bid)) => bid,
            (None, None) => {
                info!("Skipping candle with no price data: symbol_id={}", symbol_id);
                return Ok(());
            }
        };
        
        info!(
            "New candle: symbol_id={}, time={}, price={:.2} (ask={:?}, bid={:?})",
            symbol_id,
            timestamp.format("%H:%M:%S"),
            price,
            ask_price,
            bid_price
        );
        
        // Update cache for this symbol
        self.update_symbol_data(symbol_id, timestamp, price)?;
        
        // Calculate correlations if we have enough symbols
        if self.data_cache.len() >= 2 {
            self.calculate_and_print_correlations()?;
        }
        
        Ok(())
    }

    fn update_symbol_data(
        &mut self,
        symbol_id: i32,
        timestamp: DateTime<Utc>,
        price: f64,
    ) -> Result<()> {
        // Add new data point or create new DataFrame
        let cutoff_time = Utc::now() - Duration::minutes(self.window_minutes as i64);
        
        if let Some(df) = self.data_cache.get_mut(&symbol_id) {
            // Append new row and remove old data
            // (Implementation depends on Polars version)
            // For now, recreate DataFrame
            let mut timestamps: Vec<i64> = df.column("timestamp")?.i64()?.into_no_null_iter().collect();
            let mut prices: Vec<f64> = df.column("price")?.f64()?.into_no_null_iter().collect();
            
            timestamps.push(timestamp.timestamp_millis());
            prices.push(price);
            
            // Filter old data
            let cutoff_ms = cutoff_time.timestamp_millis();
            let filtered: Vec<(i64, f64)> = timestamps
                .into_iter()
                .zip(prices.into_iter())
                .filter(|(t, _)| *t >= cutoff_ms)
                .collect();
            
            let new_df = DataFrame::new(vec![
                Series::new("timestamp", filtered.iter().map(|(t, _)| *t).collect::<Vec<_>>()),
                Series::new("price", filtered.iter().map(|(_, p)| *p).collect::<Vec<_>>()),
            ])?;
            
            *df = new_df;
        } else {
            // Create new DataFrame for this symbol
            let df = DataFrame::new(vec![
                Series::new("timestamp", vec![timestamp.timestamp_millis()]),
                Series::new("price", vec![price]),
            ])?;
            self.data_cache.insert(symbol_id, df);
        }
        
        Ok(())
    }

    fn calculate_and_print_correlations(&self) -> Result<()> {
        let symbol_ids: Vec<&i32> = self.data_cache.keys().collect();
        
        println!("\n=== Correlation Matrix ===");
        println!("Symbols: {:?}", symbol_ids);
        
        // Calculate pairwise correlations
        for i in 0..symbol_ids.len() {
            for j in i + 1..symbol_ids.len() {
                let symbol1 = *symbol_ids[i];
                let symbol2 = *symbol_ids[j];
                
                if let (Some(df1), Some(df2)) = (
                    self.data_cache.get(&symbol1),
                    self.data_cache.get(&symbol2),
                ) {
                    // Align DataFrames by timestamp and calculate correlation
                    if let Ok(correlation) = self.calculate_correlation(df1, df2) {
                        println!(
                            "Correlation between {} and {}: {:.4}",
                            symbol1, symbol2, correlation
                        );
                    }
                }
            }
        }
        
        Ok(())
    }

    fn calculate_correlation(&self, df1: &DataFrame, df2: &DataFrame) -> Result<f64> {
        // Rename columns in df2 to avoid conflicts
        let df2_renamed = df2.clone().lazy()
            .rename(["price"], ["price_right"])
            .collect()?;
        
        // Join DataFrames on timestamp
        let joined = df1.join(
            &df2_renamed,
            ["timestamp"],
            ["timestamp"],
            JoinArgs::new(JoinType::Inner),
        )?;
        
        // Get price columns
        let prices1 = joined.column("price")?.f64()?;
        let prices2 = joined.column("price_right")?.f64()?;
        
        // Calculate correlation
        if prices1.len() >= self.min_data_points {
            // Calculate correlation manually
            let prices1_vec: Vec<f64> = prices1.into_no_null_iter().collect();
            let prices2_vec: Vec<f64> = prices2.into_no_null_iter().collect();
            let correlation = calculate_pearson_correlation(&prices1_vec, &prices2_vec);
            Ok(correlation)
        } else {
            Ok(f64::NAN)
        }
    }
}

fn calculate_pearson_correlation(x: &[f64], y: &[f64]) -> f64 {
    if x.len() != y.len() || x.is_empty() {
        return f64::NAN;
    }
    
    let n = x.len() as f64;
    let sum_x: f64 = x.iter().sum();
    let sum_y: f64 = y.iter().sum();
    let sum_xy: f64 = x.iter().zip(y.iter()).map(|(a, b)| a * b).sum();
    let sum_x2: f64 = x.iter().map(|a| a * a).sum();
    let sum_y2: f64 = y.iter().map(|b| b * b).sum();
    
    let numerator = n * sum_xy - sum_x * sum_y;
    let denominator = ((n * sum_x2 - sum_x * sum_x) * (n * sum_y2 - sum_y * sum_y)).sqrt();
    
    if denominator == 0.0 {
        f64::NAN
    } else {
        numerator / denominator
    }
}