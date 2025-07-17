# Server Setup

### Basic

see: https://github.com/kazukingh01/kkenv/blob/8ea7a6b7ffee064498d0df54f1849b1b75828157/ubuntu/README.md#server-basic-setup

### Database

##### MongoDB Install

MONGO_VER="8.0.11"
see: https://github.com/kazukingh01/kkpsgre/blob/e19504564aab73e61450dcfc4f3d79b40f00235d/README.md#mongodb--stand-alone-

##### Schema

```bash
cd && mongosh admin -u "admin" -p `cat ~/passmongo.txt` --port ${PORTMS} --eval 'load("./kkcrypto/src/db/schema.mongo.js");'
```

##### Sharding

```bash
mongosh admin -u "admin" -p `cat ~/passmongo.txt` --port ${PORTMS} --eval 'sh.enableSharding("trade");'
mongosh admin -u "admin" -p `cat ~/passmongo.txt` --port ${PORTMS} --eval 'sh.shardCollection("trade.candles_1s", {"metadata": 1});'
```

# HyperLiquid

### Check list of Perpetual coin

```bash
curl -s -X POST https://api.hyperliquid.xyz/info -H 'Content-Type: application/json' -d '{"type":"meta"}' | jq '.universe[].name'
```

### Check list of Spot coin

```bash
python -c "import requests, json; resp = requests.post('https://api.hyperliquid.xyz/info', json={'type': 'spotMeta'}).json(); id_to_token = {tok['index']: tok['name'] for tok in resp['tokens']}; pairs = [(item['name'],f\"{id_to_token[item['tokens'][0]]}/{id_to_token[item['tokens'][1]]}\") for item in resp['universe']]; [print(f'{x}: {y}') for x, y in dict(pairs).items()]"
```

# Script

```bash
cargo clean --package kkcrypto
cargo build
./target/debug/bybit       --raw-freq 100 --spot    --update --symbols BTCUSDT,ETHUSDT,XRPUSDT,BNBUSDT,SOLUSDT 
./target/debug/bybit       --raw-freq 100 --linear  --update --symbols BTCUSDT,ETHUSDT,XRPUSDT,BNBUSDT,SOLUSDT 
./target/debug/bybit       --raw-freq 100 --inverse --update --symbols BTCUSD,ETHUSD,XRPUSD,SOLUSD             
./target/debug/binance     --raw-freq 100 --spot    --update --symbols BTCUSDT,ETHUSDT,XRPUSDT,BNBUSDT,SOLUSDT 
./target/debug/binance     --raw-freq 100 --linear  --update --symbols BTCUSDT,ETHUSDT,XRPUSDT,BNBUSDT,SOLUSDT 
./target/debug/binance     --raw-freq 100 --inverse --update --symbols BTCUSD_PERP,ETHUSD_PERP,XRPUSD_PERP,BNBUSD_PERP,SOLUSD_PERP 
./target/debug/hyperliquid --raw-freq 100 --linear  --update --symbols BTC,ETH,XRP,BNB,SOL,HYPE 
```
