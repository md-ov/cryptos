#!/bin/bash
now="$(date +'%Y-%m-%d')"
cd /d/ws/cryptos/cryptos/krakenc/

if ! [ -f D:\\ws\\cryptos\\data\\trades\\xbt-before-$now ]; then
	echo "Folder xbt-before-$now does not exist => retrieve trades date from last timestamp"
	rm ./out/trades/*.csv
	echo "./out/trades/*.csv removed"

	node main-get-t-a.js \{\"asset\":\"XBT\",\"currency\":\"EUR\",\"n\":1000\}
	echo "main-get-t-a.js for XBT EUR done"

	mkdir D:\\ws\\cryptos\\data\\trades\\xbt-before-$now\\
	echo "trades\\xbt-before-$now\\ maked"

	mv -n /d/ws/cryptos/cryptos/krakenc/out/trades/*.csv D:\\ws\\cryptos\\data\\trades\\xbt-before-$now\\
	echo "krakenc/out/trades/*.csv moved to D:\\ws\\cryptos\\data\\trades\\xbt-before-$now\\"
	
	echo "Retrieve trades date from last timestamp done"

	echo "Starting To parquets with spark for xbt-before-$now..."
	cryptos-apps to-parquets-from-csv --master local --api trades --input-dir D:\\ws\\cryptos\\data\\trades\\xbt-before-$now --parquets-dir file:///D:\\ws\\cryptos\\data\\parquets --minimum 1
	echo "To parquets with spark for xbt-before-$now done"
fi

echo "Starting get ohlc and today trades at $now, please wait..."
rm ./out/ohlc/*.csv
echo "./out/ohlc/*.csv removed"
rm ./out/trades/today/*.csv
echo "./out/trades/today/*.csv removed"

node main-get-t-today-a.js \{\"asset\":\"XBT\",\"currency\":\"EUR\",\"n\":1000\}
echo "main-get-t-today-a.js for XBT EUR done"
node main-get-o-a.js \{\"asset\":\"XBT\",\"currency\":\"EUR\"} 
echo "main-get-o-a.js for XBT EUR done"

rm -r D:\\ws\\cryptos\\data\\trades\\xbt-today-$now\\
echo "trades\\xbt-today-$now\\ removed"
mkdir D:\\ws\\cryptos\\data\\trades\\xbt-today-$now\\
echo "trades\\xbt-today-$now\\ maked"

mv -n /d/ws/cryptos/cryptos/krakenc/out/trades/today/*.csv D:\\ws\\cryptos\\data\\trades\\xbt-today-$now\\
echo "krakenc/out/trades/today/*.csv moved to D:\\ws\\cryptos\\data\\trades\\xbt-today-$now\\"
mv /d/ws/cryptos/cryptos/krakenc/out/ohlc/*.csv D:\\ws\\cryptos\\data\\ohlc\\xbt
echo "krakenc/out/ohlc/*.csv moved to D:\\ws\\cryptos\\data\\ohlc\\xbt"

rm -r /d/ws/cryptos/data/parquets/XBT/EUR/OHLC/parquet
echo "parquets/XBT/EUR/OHLC/parquet removed"
rm -r /d/ws/cryptos/data/parquets/XBT/EUR/TRADES/today/*
echo "parquets/XBT/EUR/TRADES/today/* removed"
mkdir D:\\ws\\cryptos\\data\\parquets\\XBT\\EUR\\TRADES\\today\\parquet\\
echo "parquets\\XBT\\EUR\\TRADES\\today\\parquet\\ maked"

echo "Get kraken at $now successfully completed"

echo "Starting To parquets with spark for ohlc xbt..."
cryptos-apps to-parquets-from-csv --master local --api ohlc --input-dir D:\\ws\\cryptos\\data\\ohlc\\xbt --parquets-dir file:///D:\\ws\\cryptos\\data\\parquets --minimum 500
echo "To parquets with spark for ohlc xbt done"

echo "Starting To parquets with spark for trades xbt-today-$now..."
cryptos-apps to-parquets-from-today-csv --master local --api trades --input-dir D:\\ws\\cryptos\\data\\trades\\xbt-today-$now --parquets-dir file:///D:\\ws\\cryptos\\data\\parquets --minimum 1
echo "To parquets with spark for trades xbt-today-$now done"

cmd