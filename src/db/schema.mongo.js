// metadata: { ym: 202401, symbol: 1 } ym: year-month, symbol: symbol index reffered to master csv file.
db.getSiblingDB("trade").createCollection("candles_1s",  { timeseries: {timeField: "unixtime", metaField: "metadata", granularity: "seconds" }})
db.getSiblingDB("trade").createCollection("candles_5s",  { timeseries: {timeField: "unixtime", metaField: "metadata", granularity: "seconds" }})
db.getSiblingDB("trade").createCollection("candles_10s", { timeseries: {timeField: "unixtime", metaField: "metadata", granularity: "seconds" }})
db.getSiblingDB("trade").createCollection("candles_60s", { timeseries: {timeField: "unixtime", metaField: "metadata", granularity: "seconds" }})

// db.candles_5s.deleteMany({})
// db.candles_5s.drop()

sh.shardCollection("trade.candles_1s",  {"metadata": 1});
sh.shardCollection("trade.candles_5s",  {"metadata": 1});
sh.shardCollection("trade.candles_10s", {"metadata": 1});
sh.shardCollection("trade.candles_60s", {"metadata": 1});