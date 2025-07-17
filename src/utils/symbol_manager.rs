use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use anyhow::Result;

pub struct SymbolManager {
    symbol_map: HashMap<(String, String, String), i32>, // (exchange, symbol, market_type) -> symbol_id
}

impl SymbolManager {
    pub fn new() -> Result<Self> {
        let mut symbol_map = HashMap::new();
        
        // master.csvを読み込む
        let file = File::open("src/db/master.csv")?;
        let reader = BufReader::new(file);
        
        for line in reader.lines().skip(1) { // ヘッダー行をスキップ
            let line = line?;
            let parts: Vec<&str> = line.split(',').collect();
            
            if parts.len() >= 4 {
                let symbol_id: i32 = parts[0].parse()?;
                let symbol_name = parts[1].to_string();
                let exchange = parts[2].to_string();
                let market_type = parts[3].to_string();
                
                symbol_map.insert((exchange, symbol_name, market_type), symbol_id);
            }
        }
        
        Ok(Self { symbol_map })
    }
    
    pub fn get_symbol_id(&self, exchange: &str, symbol: &str, market_type: &str) -> Option<i32> {
        self.symbol_map.get(&(exchange.to_string(), symbol.to_string(), market_type.to_string())).copied()
    }
}

// グローバルインスタンス
lazy_static::lazy_static! {
    pub static ref SYMBOL_MANAGER: SymbolManager = SymbolManager::new().expect("Failed to load symbol manager");
}