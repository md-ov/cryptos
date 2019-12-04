#!/bin/bash
now="$(date +'%Y-%m-%d')"
echo "Starting get kraken at $now, please wait..."
cd /c/ws/ov/cryptos/krakenc/
if ! [ -f C:\\ws\\ov\\data\\cryptos\\trades\\xbt-before-$now ]; then
	echo "Folder does not exist"
	rm ./out/trades/*.csv
	node main-get-t-a.js \{\"asset\":\"XBT\",\"currency\":\"EUR\",\"n\":1000\}
	mkdir C:\\ws\\ov\\data\\cryptos\\trades\\xbt-before-$now\\
	mv -n /c/ws/ov/cryptos/krakenc/out/trades/*.csv C:\\ws\\ov\\data\\cryptos\\trades\\xbt-before-$now\\
	cryptos-apps to-parquets-from-csv --master local --api trades --input-dir C:\\ws\\ov\\data\\cryptos\\trades\\xbt-before-$now --parquets-dir file:///C:\\ws\\ov\\data\\cryptos\\parquets --minimum 1
fi

rm ./out/ohlc/*.csv
rm ./out/trades/today/*.csv
node main-get-t-today-a.js \{\"asset\":\"XBT\",\"currency\":\"EUR\",\"n\":1000\}
node main-get-o-a.js \{\"asset\":\"XBT\",\"currency\":\"EUR\"} 
rm -r C:\\ws\\ov\\data\\cryptos\\trades\\xbt-today-$now\\
mkdir C:\\ws\\ov\\data\\cryptos\\trades\\xbt-today-$now\\
mv -n /c/ws/ov/cryptos/krakenc/out/trades/today/*.csv C:\\ws\\ov\\data\\cryptos\\trades\\xbt-today-$now\\
mv /c/ws/ov/cryptos/krakenc/out/ohlc/*.csv C:\\ws\\ov\\data\\cryptos\\data\\ohlc\\xbt
rm -r /c/ws/ov/data/cryptos/parquets/XBT/EUR/OHLC/parquet
rm -r /c/ws/ov/data/cryptos/parquets/XBT/EUR/TRADES/today/*
mkdir C:\\ws\\ov\\data\\cryptos\\parquets\\XBT\\EUR\\TRADES\\today\\parquet\\

cryptos-apps to-parquets-from-csv --master local --api ohlc --input-dir C:\\ws\\ov\\data\\cryptos\\ohlc\\xbt --parquets-dir file:///C:\\ws\\ov\\data\\cryptos\\parquets --minimum 500
cryptos-apps to-parquets-from-today-csv --master local --api trades --input-dir C:\\ws\\ov\\data\\cryptos\\trades\\xbt-today-$now --parquets-dir file:///C:\\ws\\ov\\data\\cryptos\\parquets --minimum 1

cmd