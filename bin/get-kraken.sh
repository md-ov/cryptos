#!/bin/bash
now="$(date +'%Y-%m-%d')"
echo "Starting get kraken at $now, please wait..."
cd /d/ws/cryptos/cryptos/krakenc/
node main-get-t-a.js \{\"asset\":\"XBT\",\"currency\":\"EUR\",\"n\":1000\}
node main-get-t-today-a.js \{\"asset\":\"XBT\",\"currency\":\"EUR\",\"n\":1000\}
node main-get-o-a.js \{\"asset\":\"XBT\",\"currency\":\"EUR\"} 
mkdir D:\\ws\\cryptos\\data\\trades\\xbt-before-$now\\
mkdir D:\\ws\\cryptos\\data\\trades\\xbt-today-$now\\
mv -n /d/ws/cryptos/cryptos/krakenc/out/trades/*.csv D:\\ws\\cryptos\\data\\trades\\xbt-before-$now\\
mv -n /d/ws/cryptos/cryptos/krakenc/out/trades/today/*.csv D:\\ws\\cryptos\\data\\trades\\xbt-today-$now\\
mv /d/ws/cryptos/cryptos/krakenc/out/ohlc/*.csv D:\\ws\\cryptos\\data\\ohlc\\xbt
rm -r /d/ws/cryptos/data/parquets/XBT/EUR/OHLC/parquet
rm -r /d/ws/cryptos/data/parquets/XBT/EUR/TRADES/today/*
mkdir D:\\ws\\cryptos\\data\\parquets\\XBT\\EUR\\TRADES\\today\\parquet\\

cryptos-apps to-parquets-from-csv --master local --api ohlc --input-dir D:\\ws\\cryptos\\data\\ohlc\\xbt --parquets-dir file:///D:\\ws\\cryptos\\data\\parquets --minimum 500
cryptos-apps to-parquets-from-csv --master local --api trades --input-dir D:\\ws\\cryptos\\data\\trades\\xbt-before-$now --parquets-dir file:///D:\\ws\\cryptos\\data\\parquets --minimum 1
cryptos-apps to-parquets-from-today-csv --master local --api trades --input-dir D:\\ws\\cryptos\\data\\trades\\xbt-today-$now --parquets-dir file:///D:\\ws\\cryptos\\data\\parquets --minimum 1

cmd