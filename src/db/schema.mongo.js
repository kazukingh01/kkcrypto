// metadata: { ym: 202401, symbol: 1 } ym: year-month, symbol: symbol index reffered to master csv file.
db.getSiblingDB("trade").createCollection("candles_1s", { timeseries: {timeField: "unixtime", metaField: "metadata", granularity: "seconds" }})

