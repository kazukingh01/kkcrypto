#!/bin/bash

# プロセス監視・再起動スクリプト
# cron で毎分実行する

cd "$(dirname "$0")"

# --update フラグの処理
UPDATE_FLAG=""
if [[ "$1" == "--update" ]]; then
    UPDATE_FLAG="--update"
fi

# ログファイル
MONITOR_LOG="logs/monitor.log"
mkdir -p logs

log_msg() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') $1" >> "$MONITOR_LOG"
}

# ログローテーション関数
rotate_log() {
    local logfile="$1"
    local max_size_mb="$2"  # MB単位
    
    if [[ -f "$logfile" ]]; then
        # ファイルサイズをMB単位で取得
        local size_mb=$(du -m "$logfile" | cut -f1)
        
        if (( size_mb > max_size_mb )); then
            # ファイルの最後の1000行のみ残す
            tail -n 1000 "$logfile" > "${logfile}.tmp"
            mv "${logfile}.tmp" "$logfile"
            log_msg "Rotated log: $logfile (was ${size_mb}MB, kept last 1000 lines)"
        fi
    fi
}

# プロセス定義 (name:pidfile:command)
PROCESSES=(
    "bybit_spot:pids/bybit_spot.pid:./target/debug/bybit                       --raw-freq 100 --spot    $UPDATE_FLAG --symbols BTCUSDT,ETHUSDT,XRPUSDT,BNBUSDT,SOLUSDT"
    "bybit_linear:pids/bybit_linear.pid:./target/debug/bybit                   --raw-freq 100 --linear  $UPDATE_FLAG --symbols BTCUSDT,ETHUSDT,XRPUSDT,BNBUSDT,SOLUSDT"
    "bybit_inverse:pids/bybit_inverse.pid:./target/debug/bybit                 --raw-freq 100 --inverse $UPDATE_FLAG --symbols BTCUSD,ETHUSD,XRPUSD,SOLUSD"
    "binance_spot:pids/binance_spot.pid:./target/debug/binance                 --raw-freq 100 --spot    $UPDATE_FLAG --symbols BTCUSDT,ETHUSDT,XRPUSDT,BNBUSDT,SOLUSDT"
    "binance_linear:pids/binance_linear.pid:./target/debug/binance             --raw-freq 100 --linear  $UPDATE_FLAG --symbols BTCUSDT,ETHUSDT,XRPUSDT,BNBUSDT,SOLUSDT"
    "binance_inverse:pids/binance_inverse.pid:./target/debug/binance           --raw-freq 100 --inverse $UPDATE_FLAG --symbols BTCUSD_PERP,ETHUSD_PERP,XRPUSD_PERP,BNBUSD_PERP,SOLUSD_PERP"
    "hyperliquid_linear:pids/hyperliquid_linear.pid:./target/debug/hyperliquid --raw-freq 100 --linear  $UPDATE_FLAG --symbols BTC,ETH,XRP,BNB,SOL,HYPE"
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

# ログローテーション実行（50MB以上で実行）
for process_def in "${PROCESSES[@]}"; do
    IFS=':' read -r name pidfile command <<< "$process_def"
    rotate_log "logs/${name}.log" 50
done
rotate_log "$MONITOR_LOG" 10

# 各プロセスをチェック
for process_def in "${PROCESSES[@]}"; do
    IFS=':' read -r name pidfile command <<< "$process_def"
    
    if [[ -f "$pidfile" ]]; then
        pid=$(cat "$pidfile")
        
        # プロセスが実行中かチェック
        if kill -0 "$pid" 2>/dev/null; then
            # プロセスは生きている
            continue
        else
            # プロセスが死んでいる
            log_msg "Process $name (PID: $pid) is dead. Restarting..."
            restart_process "$name" "$pidfile" "$command"
        fi
    else
        # PIDファイルが存在しない（初回起動または異常終了）
        log_msg "PIDfile for $name not found. Starting..."
        restart_process "$name" "$pidfile" "$command"
    fi
done