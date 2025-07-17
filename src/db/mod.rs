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
        
        // 常にJSONを出力
        println!("[DB-INSERT] {}", serde_json::to_string(&doc)?); 
        
        // リアル接続がある場合のみ実際に挿入
        if !self.is_dummy {
            if let Some(ref database) = self.database {
                let collection = database.collection::<Document>("candles_1s");
                tracing::info!("Attempting to insert into MongoDB: database=trade, collection=candles_1s");
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