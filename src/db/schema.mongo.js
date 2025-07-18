// metadata: { ym: 202401, symbol: 1 } ym: year-month, symbol: symbol index reffered to master csv file.
db.getSiblingDB("trade").createCollection("candles_1s", { timeseries: {timeField: "unixtime", metaField: "metadata", granularity: "seconds" }})
db.getSiblingDB("trade").createCollection("candles_5s", { timeseries: {timeField: "unixtime", metaField: "metadata", granularity: "seconds" }})

sh.shardCollection("trade.candles_1s", {"metadata": 1});
sh.shardCollection("trade.candles_5s", {"metadata": 1});