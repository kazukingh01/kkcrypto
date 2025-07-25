use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use clap::Parser;
use mongodb::{
    bson::{doc, Document},
    Client,
};
use polars::prelude::*;
use std::collections::HashMap;
use std::time::Instant;
use tracing::error;
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
                .unwrap_or_else(|_| "info".into()),
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

    println!("Connected to MongoDB");

    // Verify database connection
    println!("[STARTUP] Verifying database connection...");
    let test_filter = doc! { 
        "unixtime": { "$gte": mongodb::bson::DateTime::from_millis(Utc::now().timestamp_millis() - 60000) }
    };
    match collection.find_one(test_filter).await {
        Ok(Some(_)) => println!("[STARTUP] Database connection verified"),
        Ok(None) => println!("[WARNING] No recent data found in database"),
        Err(e) => {
            println!("[ERROR] Failed to connect to database: {}", e);
            return Err(e.into());
        }
    }

    // Use interval timer approach
    println!("Starting interval timer mode (5 second intervals)...");
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
    
    loop {
        // Wait for next tick
        interval.tick().await;
        
        // Create new calculator instance for stateless processing
        let mut calculator = CorrelationCalculator::new(
            collection.clone(),
            args.window_minutes,
            args.min_data_points,
        );
        
        // Load all data for the window period
        let start_time = Instant::now();
        match calculator.load_initial_data().await {
            Ok(_) => {
                let elapsed = start_time.elapsed();
                println!("[TIMER] Data load and processing: {:?}", elapsed);
                
                // Calculate and print correlations
                if let Some(ref df) = calculator.data_df {
                    if df.width() > 2 { // timestamp + at least 2 price columns
                        if let Err(e) = calculator.calculate_and_print_correlations() {
                            error!("Error calculating correlations: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                error!("Error loading data: {}", e);
            }
        }
    }
    
    #[allow(unreachable_code)]
    Ok(())
}

struct CorrelationCalculator {
    collection: mongodb::Collection<Document>,
    window_minutes: u32,
    min_data_points: usize,
    data_df: Option<DataFrame>, // Single DataFrame with all symbols
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
            data_df: None,
        }
    }

    async fn load_initial_data(&mut self) -> Result<()> {
        let timer_start = Instant::now();
        let now = Utc::now();
        let start_time = now - Duration::minutes(self.window_minutes as i64);
        let start_time_ms = start_time.timestamp_millis();
        
        println!("Current time: {} ({}ms)", now.format("%Y-%m-%d %H:%M:%S"), now.timestamp_millis());
        println!("Loading data from {} ({}ms)", start_time.format("%Y-%m-%d %H:%M:%S"), start_time_ms);
        
        // Query for all data in the window (using DateTime object)
        let filter = doc! {
            "unixtime": { "$gte": mongodb::bson::DateTime::from_millis(start_time_ms) }
        };
        
        let query_start = Instant::now();
        let mut cursor = self.collection.find(filter).await?;
        let query_elapsed = query_start.elapsed();
        println!("[TIMER] MongoDB query execution: {:?}", query_elapsed);
        let mut data_by_symbol: HashMap<i32, Vec<(DateTime<Utc>, f64)>> = HashMap::new();
        let mut total_docs = 0;
        
        // Collect data by symbol
        while cursor.advance().await? {
            let raw_doc = cursor.current();
            let doc: Document = raw_doc.try_into()?;            
            if let (Ok(symbol_id), Ok(timestamp_ms)) = (
                doc.get_document("metadata")?.get_i32("symbol"),
                doc.get_datetime("unixtime").map(|dt| dt.timestamp_millis()),
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
        
        println!("Loaded {} documents for {} symbols", total_docs, data_by_symbol.len());
        println!("Symbols loaded: {:?}", data_by_symbol.keys().collect::<Vec<_>>());
        if data_by_symbol.is_empty() {
            println!("WARNING: No data found in the last {} minutes!", self.window_minutes);
        }
        
        // Create unified DataFrame with all symbols
        let end_time = Utc::now();
        
        // A. MongoDBデータからDataFrameを作成
        let mongo_df = self.create_dataframe_from_mongo_data(data_by_symbol)?;
        
        // B. 時間軸を作成してjoin + forward fill
        self.data_df = Some(self.create_filled_dataframe_with_timeaxis(mongo_df, start_time, end_time, 5)?);
        
        println!("Created unified DataFrame with {} symbols", 
            self.data_df.as_ref().unwrap().width() - 1); // -1 for timestamp column
        
        let total_elapsed = timer_start.elapsed();
        println!("[TIMER] Total initial data load time: {:?}", total_elapsed);
        
        Ok(())
    }

    // A. MongoDBデータからDataFrameを作成
    fn create_dataframe_from_mongo_data(
        &self,
        data_by_symbol: HashMap<i32, Vec<(DateTime<Utc>, f64)>>,
    ) -> Result<DataFrame> {
        let mut all_rows = Vec::new();
        
        for (symbol_id, data) in data_by_symbol {
            println!("Processing symbol {}: {} data points", symbol_id, data.len());
            
            for (timestamp, price) in data {
                all_rows.push((timestamp.timestamp_millis(), symbol_id, price));
            }
        }
        
        if all_rows.is_empty() {
            return Ok(DataFrame::empty());
        }
        
        // Sort by timestamp
        all_rows.sort_by_key(|(ts, _, _)| *ts);
        
        let timestamps: Vec<i64> = all_rows.iter().map(|(ts, _, _)| *ts).collect();
        let symbol_ids: Vec<i32> = all_rows.iter().map(|(_, sid, _)| *sid).collect();
        let prices: Vec<f64> = all_rows.iter().map(|(_, _, p)| *p).collect();
        
        Ok(DataFrame::new(vec![
            Series::new("timestamp".into(), timestamps).into(),
            Series::new("symbol_id".into(), symbol_ids).into(),
            Series::new("price".into(), prices).into(),
        ])?)
    }
    
    // B. 時間軸を作成してjoin + forward fill
    fn create_filled_dataframe_with_timeaxis(
        &self,
        data_df: DataFrame,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        interval_seconds: i64,
    ) -> Result<DataFrame> {
        // Align timestamps
        let start_millis = start_time.timestamp_millis();
        let interval_millis = interval_seconds * 1000;
        let aligned_start_millis = (start_millis / interval_millis) * interval_millis;
        let aligned_start = DateTime::from_timestamp_millis(aligned_start_millis).unwrap();
        
        let end_millis = end_time.timestamp_millis();
        let aligned_end_millis = ((end_millis + interval_millis - 1) / interval_millis) * interval_millis;
        let aligned_end = DateTime::from_timestamp_millis(aligned_end_millis).unwrap();
        
        // Create complete time series
        let mut timestamps = vec![];
        let mut current = aligned_start;
        while current <= aligned_end {
            timestamps.push(current.timestamp_millis());
            current = current + Duration::seconds(interval_seconds);
        }
        
        // Create base time DataFrame
        let base_time_df = DataFrame::new(vec![
            Series::new("timestamp".into(), timestamps.clone()).into()
        ])?;
        
        if data_df.is_empty() {
            return Ok(base_time_df);
        }
        
        // Get unique symbol_ids from data
        let symbol_ids: Vec<i32> = data_df.column("symbol_id")?
            .i32()?
            .into_no_null_iter()
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        
        // Pivot data to wide format (timestamp -> symbol columns)
        let mut result_columns: Vec<Column> = vec![
            Series::new("timestamp".into(), timestamps.clone()).into()
        ];
        
        for symbol_id in symbol_ids {
            // Filter data for this symbol and remove duplicates
            let symbol_data = data_df.clone().lazy()
                .filter(col("symbol_id").eq(lit(symbol_id)))
                .select([col("timestamp"), col("price")])
                .group_by([col("timestamp")])
                .agg([col("price").first()]) // 重複タイムスタンプがある場合は最初の値を採用
                .collect()?;
            
            // Join with base time
            let joined = base_time_df.join(
                &symbol_data,
                ["timestamp"],
                ["timestamp"],
                JoinArgs::new(JoinType::Left),
                None,
            )?;
            
            // Assert: join結果の行数が基軸の時間軸と一致することを確認
            let base_height = base_time_df.height();
            let joined_height = joined.height();
            if base_height != joined_height {
                println!("ERROR: Join mismatch for symbol_{}:", symbol_id);
                println!("  Base time axis: {} rows", base_height);
                println!("  Symbol data: {} rows", symbol_data.height());
                println!("  Joined result: {} rows", joined_height);
                
                // デバッグ: symbol_dataの重複チェック
                let unique_timestamps = symbol_data.column("timestamp")?
                    .unique()?
                    .len();
                println!("  Symbol data unique timestamps: {}", unique_timestamps);
                
                panic!("Join assertion failed: base_height({}) != joined_height({})", 
                    base_height, joined_height);
            }
            
            // Get price column and add to result
            let price_series = joined.column("price")?.clone();
            let column_name = format!("symbol_{}", symbol_id);
            result_columns.push(price_series.with_name(column_name.as_str().into()).into());
        }
        
        let mut result_df = DataFrame::new(result_columns)?;
        
        // Forward fill all symbol columns
        let symbol_columns: Vec<String> = result_df.get_column_names()
            .iter()
            .filter(|name| name.starts_with("symbol_"))
            .map(|s| s.to_string())
            .collect();
        
        for col_name in &symbol_columns {
            result_df = result_df.lazy()
                .with_columns([
                    col(col_name).fill_null_with_strategy(FillNullStrategy::Forward(None))
                ])
                .collect()?;
        }
        
        // Show null counts after forward fill
        let mut null_info = vec![];
        for col_name in &symbol_columns {
            let null_count = result_df.column(col_name)?.null_count();
            null_info.push(format!("{}:{}", col_name, null_count));
        }
        println!("Null counts after forward fill: {}", null_info.join(", "));
        
        Ok(result_df)
    }

    fn calculate_and_print_correlations(&self) -> Result<()> {
        if let Some(ref df) = self.data_df {
            let symbol_columns: Vec<String> = df.get_column_names()
                .iter()
                .filter(|name| name.starts_with("symbol_"))
                .map(|s| s.to_string())
                .collect();
            
            println!("\n=== Correlation Matrix ===");
            println!("Symbols: {:?}", symbol_columns);
            
            // Calculate pairwise correlations
            for i in 0..symbol_columns.len() {
                for j in i + 1..symbol_columns.len() {
                    let col1 = &symbol_columns[i];
                    let col2 = &symbol_columns[j];
                    
                    if let Ok(correlation) = self.calculate_correlation_unified(df, col1, col2) {
                        let symbol1 = col1.replace("symbol_", "");
                        let symbol2 = col2.replace("symbol_", "");
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

    fn calculate_correlation_unified(&self, df: &DataFrame, col1: &str, col2: &str) -> Result<f64> {
        // Select only the two columns and drop nulls
        let correlation_df = df.select([col1, col2])?
            .lazy()
            .drop_nulls(None)
            .collect()?;
        
        if correlation_df.height() < self.min_data_points {
            println!("  Insufficient data points: {} (need {})", correlation_df.height(), self.min_data_points);
            return Ok(f64::NAN);
        }
        
        // Convert to ndarray and calculate correlation
        match correlation_df.to_ndarray::<Float64Type>(IndexOrder::Fortran) {
            Ok(array) => {
                use ndarray_stats::CorrelationExt;
                let transposed = array.t();
                match transposed.pearson_correlation() {
                    Ok(corr_matrix) => {
                        let correlation = corr_matrix[[0, 1]];
                        Ok(correlation)
                    },
                    Err(e) => {
                        println!("  Correlation calculation error: {:?}", e);
                        Ok(f64::NAN)
                    }
                }
            },
            Err(e) => {
                println!("  Failed to convert DataFrame to ndarray: {}", e);
                Ok(f64::NAN)
            }
        }
    }
}