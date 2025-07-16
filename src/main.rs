fn main() {
    eprintln!("Use specific binaries:");
    eprintln!("  cargo run --bin bybit -- --symbols BTCUSDT");
    eprintln!("  cargo run --bin binance -- --symbols BTCUSDT");
    eprintln!("");
    eprintln!("Add --update flag to write to database:");
    eprintln!("  cargo run --bin bybit -- --symbols BTCUSDT --update");
}