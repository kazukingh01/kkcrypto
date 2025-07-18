#!/bin/bash

# プロセス監視・再起動スクリプト
# cron で毎分実行する

cd "$(dirname "$0")"

# ヘルプ表示
show_help() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS]

Cryptocurrency data collection process monitor and manager

OPTIONS:
    --test      Run in test mode (dummy DB connection, no actual writes)
    --update    Enable database updates (production mode)
    --kill      Stop all monitored processes
    --help      Show this help message

DESCRIPTION:
    This script monitors and automatically restarts cryptocurrency data collection
    processes. It runs 7 processes for different exchanges and market types:
    - Bybit: spot, linear, inverse
    - Binance: spot, linear, inverse  
    - Hyperliquid: linear

    Logs are automatically rotated when they exceed size limits.

ENVIRONMENT VARIABLES (.env file):
    MONGODB_URL     MongoDB connection URL (required for --update mode)
                    Example: mongodb://user:pass@localhost:27017/trade?authSource=admin
    
    RUST_LOG        Rust log level configuration (optional)
                    Default: kkcrypto=info
                    Example: kkcrypto=debug for more detailed logs

EXAMPLES:
    $(basename "$0") --test    # Monitor in test mode (dummy DB)
    $(basename "$0") --update  # Monitor with database updates
    $(basename "$0") --kill    # Stop all processes

EOF
    exit 0
}

# コマンドライン引数の処理
UPDATE_FLAG=""
KILL_MODE=false

# 引数がない場合はヘルプを表示
if [[ $# -eq 0 ]]; then
    show_help
fi

if [[ "$1" == "--help" ]] || [[ "$1" == "-h" ]]; then
    show_help
elif [[ "$1" == "--test" ]]; then
    UPDATE_FLAG=""  # ダミーモード
elif [[ "$1" == "--update" ]]; then
    UPDATE_FLAG="--update"
elif [[ "$1" == "--kill" ]]; then
    KILL_MODE=true
else
    echo "Error: Unknown option '$1'"
    echo ""
    show_help
fi

# ログファイル
MONITOR_LOG="logs/monitor.log"
mkdir -p logs pids

log_msg() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') $1" >> "$MONITOR_LOG"
}


# プロセス定義 (name:pidfile:command)
PROCESSES=(
    "bybit_spot:pids/bybit_spot.pid:./target/debug/bybit                       --raw-freq 100 -t 1,5 --spot    $UPDATE_FLAG --symbols BTCUSDT,ETHUSDT,XRPUSDT,BNBUSDT,SOLUSDT"
    "bybit_linear:pids/bybit_linear.pid:./target/debug/bybit                   --raw-freq 100 -t 1,5 --linear  $UPDATE_FLAG --symbols BTCUSDT,ETHUSDT,XRPUSDT,BNBUSDT,SOLUSDT"
    "bybit_inverse:pids/bybit_inverse.pid:./target/debug/bybit                 --raw-freq 100 -t 1,5 --inverse $UPDATE_FLAG --symbols BTCUSD,ETHUSD,XRPUSD,SOLUSD"
    "binance_spot:pids/binance_spot.pid:./target/debug/binance                 --raw-freq 100 -t 1,5 --spot    $UPDATE_FLAG --symbols BTCUSDT,ETHUSDT,XRPUSDT,BNBUSDT,SOLUSDT"
    "binance_linear:pids/binance_linear.pid:./target/debug/binance             --raw-freq 100 -t 1,5 --linear  $UPDATE_FLAG --symbols BTCUSDT,ETHUSDT,XRPUSDT,BNBUSDT,SOLUSDT"
    "binance_inverse:pids/binance_inverse.pid:./target/debug/binance           --raw-freq 100 -t 1,5 --inverse $UPDATE_FLAG --symbols BTCUSD_PERP,ETHUSD_PERP,XRPUSD_PERP,BNBUSD_PERP,SOLUSD_PERP"
    "hyperliquid_linear:pids/hyperliquid_linear.pid:./target/debug/hyperliquid --raw-freq 100 -t 1,5 --linear  $UPDATE_FLAG --symbols BTC,ETH,XRP,BNB,SOL,HYPE"
)

restart_process() {
    local name="$1"
    local pidfile="$2"
    local command="$3"
    
    log_msg "Restarting $name"
    
    # 古いPIDファイルを削除
    rm -f "$pidfile"
    
    # プロセスを再起動
    nohup $command > "logs/${name}.log" 2>&1 & echo $! > "$pidfile"
    
    log_msg "Restarted $name with PID $(cat $pidfile)"
}

# ログローテーション実行
cat > /tmp/kkcrypto_logrotate.conf << EOF
# プロセスログ（monitor.log以外）
$PWD/logs/bybit_*.log $PWD/logs/binance_*.log $PWD/logs/hyperliquid_*.log {
    size 50M
    rotate 5
    compress
    missingok
    notifempty
    copytruncate
    su $(whoami) $(whoami)
}

# monitor.log は別設定
$PWD/logs/monitor.log {
    size 10M
    rotate 3
    compress
    missingok
    notifempty
    copytruncate
    su $(whoami) $(whoami)
}
EOF

logrotate -s "$PWD/logs/logrotate.state" /tmp/kkcrypto_logrotate.conf

# --kill モードの処理
if [[ "$KILL_MODE" == "true" ]]; then
    echo "Stopping all processes..."
    for process_def in "${PROCESSES[@]}"; do
        IFS=':' read -r name pidfile command <<< "$process_def"
        
        if [[ -f "$pidfile" ]]; then
            pid=$(cat "$pidfile")
            if kill -0 "$pid" 2>/dev/null; then
                echo "Killing process $name (PID: $pid)..."
                kill "$pid"
                log_msg "Killed process $name (PID: $pid)"
            fi
            rm -f "$pidfile"
        fi
    done
    echo "All processes stopped."
    exit 0
fi

# 各プロセスをチェック
for process_def in "${PROCESSES[@]}"; do
    IFS=':' read -r name pidfile command <<< "$process_def"
    
    if [[ -f "$pidfile" ]]; then
        pid=$(cat "$pidfile")
        
        # プロセスが実行中かチェック
        if kill -0 "$pid" 2>/dev/null; then
            # プロセスは生きている
            echo "Process $name (PID: $pid) is running"
            continue
        else
            # プロセスが死んでいる
            echo "Process $name (PID: $pid) is dead. Restarting..."
            log_msg "Process $name (PID: $pid) is dead. Restarting..."
            restart_process "$name" "$pidfile" "$command"
        fi
    else
        # PIDファイルが存在しない（初回起動または異常終了）
        echo "PIDfile for $name not found. Starting..."
        log_msg "PIDfile for $name not found. Starting..."
        restart_process "$name" "$pidfile" "$command"
    fi
done