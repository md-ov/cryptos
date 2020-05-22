#!/bin/bash
today="$(date +'%Y-%m-%d')"
now="$(date +'%Y-%m-%d-%H-%M-%S')"

#asus
krakencPath="/d/ws/cryptos/cryptos/krakenc/"
dataPath="D:\\ws\\cryptos\\data\\"
dataPath1="/d/ws/cryptos/data/"

#dell
#krakencPath="/c/ws/ov/cryptos/cryptos/krakenc/"
#dataPath="C:\\ws\\ov\\cryptos\\data\\"
#dataPath1="/c/ws/ov/cryptos/data/"

cd $krakencPath

if ! [ -f ${dataPath}trades\\xbt-before-$today ]; then
	echo "Folder ${dataPath}trades\\xbt-before-$today does not exist => retrieve trades date from last timestamp"
	rm ./out/trades/*.csv
	echo "./out/trades/*.csv removed"

	node main-get-t-a.js \{\"asset\":\"XBT\",\"currency\":\"EUR\",\"n\":100000\}
	echo "main-get-t-a.js for XBT EUR done"

	mkdir ${dataPath}trades\\xbt-before-$today\\
	echo "trades\\xbt-before-$today\\ maked"

	mv -n ${krakencPath}out/trades/*.csv ${dataPath}trades\\xbt-before-$today\\
	echo "krakenc/out/trades/*.csv moved to ${dataPath}trades\\xbt-before-$today\\"

	echo "Retrieve trades date from last timestamp done"

	echo "Starting To parquets with spark for xbt-before-$today..."
	cryptos-apps to-parquets-from-csv --master local --api trades --input-dir ${dataPath}trades\\xbt-before-$today --parquets-dir file:///${dataPath}parquets --minimum 1
	echo "To parquets with spark for xbt-before-$today done"
fi

echo "Starting get ohlc and today trades at $today, please wait..."
rm ./out/ohlc/*.csv
echo "./out/ohlc/*.csv removed"
rm ./out/ohlc
echo "./out/ohlc removed"
mkdir ./out/ohlc
echo "./out/ohlc mkdir"
rm ./out/trades/today/*.csv
echo "./out/trades/today/*.csv removed"
rm ./out/trades/today
echo "./out/trades/today removed"
mkdir ./out/trades/today
echo "./out/trades/today mkdir"

node main-get-t-today-a.js \{\"asset\":\"XBT\",\"currency\":\"EUR\",\"n\":1000\}
echo "main-get-t-today-a.js for XBT EUR done"
node main-get-o-a.js \{\"asset\":\"XBT\",\"currency\":\"EUR\"}
echo "main-get-o-a.js for XBT EUR done"

rm -r ${dataPath}trades\\xbt-today-$today\\
echo "trades\\xbt-today-$today\\ removed"
mkdir ${dataPath}trades\\xbt-today-$today\\
echo "trades\\xbt-today-$today\\ maked"

mv -n ${krakencPath}out/trades/today/*.csv ${dataPath}trades\\xbt-today-$today\\
echo "krakenc/out/trades/today/*.csv moved to ${dataPath}trades\\xbt-today-$today\\"
mkdir ${dataPath}ohlc\\xbt\\$now
mv ${krakencPath}out/ohlc/*.csv ${dataPath}ohlc\\xbt\\$now
echo "krakenc/out/ohlc/*.csv moved to ${dataPath}ohlc\\xbt\\$now"

rm -r ${dataPath1}parquets/XBT/EUR/TRADES/today/*
echo "parquets/XBT/EUR/TRADES/today/* removed"
mkdir ${dataPath}parquets\\XBT\\EUR\\TRADES\\today\\parquet\\
echo "parquets\\XBT\\EUR\\TRADES\\today\\parquet\\ maked"


echo "Starting To parquets with spark for ohlc xbt..."
cryptos-apps to-parquets-from-csv --master local --api ohlc --input-dir ${dataPath}ohlc\\xbt\\$now --parquets-dir file:///${dataPath}parquets --minimum 500
echo "To parquets with spark for ohlc xbt done"

echo "Starting To parquets with spark for trades xbt-today-$today..."
cryptos-apps to-parquets-from-today-csv --master local --api trades --input-dir ${dataPath}trades\\xbt-today-$today --parquets-dir file:///${dataPath}parquets --minimum 1
echo "To parquets with spark for trades xbt-today-$today done"

echo "Get kraken successfully completed"
cmd
