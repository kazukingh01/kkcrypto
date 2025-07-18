use mongodb::{Client, Database as MongoDatabase};
use anyhow::Result;

pub struct Database {
    _client: Option<Client>,  // 将来使用予定
    database: Option<MongoDatabase>,
    is_dummy: bool,
}

impl Database {
    pub async fn new(database_url: &str, update_flag: bool) -> Result<Self> {
        use tracing::info;
        
        if update_flag {
            info!("Connecting to MongoDB: {}", database_url);
            let client = Client::with_uri_str(database_url).await?;
            let database = client.database("trade");
            
            // 接続テストを実行
            match database.run_command(mongodb::bson::doc! {"ping": 1}).await {
                Ok(_) => {
                    info!("Database initialized (real connection): database={}, status=connected", database.name());
                }
                Err(e) => {
                    tracing::error!("Database ping failed: {}", e);
                    return Err(e.into());
                }
            }
            
            Ok(Self { 
                _client: Some(client), 
                database: Some(database),
                is_dummy: false,
            })
        } else {
            // Dummy connection
            info!("Database initialized (dummy connection)");
            
            Ok(Self {
                _client: None,
                database: None,
                is_dummy: true,
            })
        }
    }


    pub async fn insert_trade_candle(&self, candle: &crate::models::trade_candle::TradeCandle) -> Result<()> {
        use mongodb::bson::Document;
        
        // Time Series形式に変換
        let doc = candle.to_timeseries_document();
        
        // コレクション名を決定
        let collection_name = match candle.period_seconds {
            1 => "candles_1s",
            5 => "candles_5s",
            10 => "candles_10s",
            30 => "candles_30s",
            60 => "candles_1m",
            300 => "candles_5m",
            900 => "candles_15m",
            1800 => "candles_30m",
            3600 => "candles_1h",
            7200 => "candles_2h",
            14400 => "candles_4h",
            86400 => "candles_1d",
            _ => {
                return Err(anyhow::anyhow!("Unsupported period: {} seconds", candle.period_seconds));
            }
        };
        
        // 常にJSONを出力
        tracing::debug!("[DB-INSERT-{}] {}", collection_name, serde_json::to_string(&doc)?); 
        
        // リアル接続がある場合のみ実際に挿入
        if !self.is_dummy {
            if let Some(ref database) = self.database {
                let collection = database.collection::<Document>(collection_name);
                tracing::debug!("Attempting to insert into MongoDB: database=trade, collection={}", collection_name);
                match collection.insert_one(doc).await {
                    Ok(result) => {
                        tracing::info!("Successfully inserted document with ID: {:?}", result.inserted_id);
                    }
                    Err(e) => {
                        tracing::error!("Failed to insert document: {}", e);
                        return Err(e.into());
                    }
                }
            } else {
                tracing::warn!("Database connection is None, cannot insert");
            }
        } else {
            tracing::debug!("Dummy mode, skipping actual database insert");
        }
        
        Ok(())
    }
}