#!/bin/bash
now="$(date +'%Y-%m-%d')"
echo "Starting get kraken at $now, please wait..."
cd /d/ws/cryptos/cryptos/krakenc/
node main-get-t-a.js \{\"asset\":\"XBT\",\"currency\":\"EUR\",\"n\":1000\}
node main-get-o-a.js \{\"asset\":\"XBT\",\"currency\":\"EUR\"} 
mkdir D:\\ws\\cryptos\\data\\trades\\xbt-before-$now\\
mv /d/ws/cryptos/cryptos/krakenc/out/trades/*.csv D:\\ws\\cryptos\\data\\trades\\xbt-before-$now\\
mv /d/ws/cryptos/cryptos/krakenc/out/*.csv D:\\ws\\cryptos\\data\\ohlc\\xbt
rm -r /d/ws/cryptos/data/parquets/XBT/EUR/OHLC/parquet
cryptos-apps to-parquets-from-csv --master local --api ohlc --input-dir D:\\ws\\cryptos\\data\\ohlc\\xbt --parquets-dir file:///D:\\ws\\cryptos\\data\\parquets --minimum 500
cryptos-apps to-parquets-from-csv --master local --api trades --input-dir D:\\ws\\cryptos\\data\\trades\\xbt-before-$now --parquets-dir file:///D:\\ws\\cryptos\\data\\parquets --minimum 1
cmd