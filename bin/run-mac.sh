#!/bin/bash
today="$(date +'%Y-%m-%d')"
now="$(date +'%Y-%m-%d-%H-%M-%S')"

#mac
home="/Users/minhdungdao"
krakencPath="${home}/ws/git/cryptos/krakenc"
dataPath="${home}/ws/data/cryptos"

sudo cd $krakencPath

if ! [ -d ${dataPath}/trades/xbt-before-$today ]; then
	echo "Folder ${dataPath}/trades/xbt-before-$today does not exist"
	echo "Retrieve trades from last timestamp BEGIN"
	rm ${krakencPath}/out/trades/*.csv
	rm ${krakencPath}/out/trades
	mkdir ${krakencPath}/out/trades
	echo "${krakencPath}out/trades/*.csv and ${krakencPath}/out/trades/ removed, ${krakencPath}/out/trades/ created"

	node ${krakencPath}/main-get-t-a.js \{\"asset\":\"XBT\",\"currency\":\"EUR\",\"n\":100000\}
	echo "main-get-t-a.js for XBT EUR done"

	mkdir ${dataPath}/trades/xbt-before-$today
	echo "${dataPath}/trades/xbt-before-$today maked"

	mv -n ${krakencPath}/out/trades/*.csv ${dataPath}/trades/xbt-before-$today/
	echo "krakenc/out/trades/*.csv moved to ${dataPath}/trades/xbt-before-$today/"

	echo "Retrieve trades from last timestamp DONE"

	echo "cryptos-apps to-parquets-from-csv trades"
	~/pack/bin/cryptos-apps to-parquets-from-csv --master local --api trades --input-dir ${dataPath}/trades/xbt-before-$today --parquets-dir ${dataPath}/parquets --minimum 1
	echo "cryptos-apps to-parquets-from-csv trades for xbt-before-$today done"
fi

echo "Starting get ohlc and today trades at $today, please wait..."
rm ${krakencPath}/out/ohlc/*.csv
echo "${krakencPath}/out/ohlc/*.csv removed"
rm ${krakencPath}/out/ohlc
echo "${krakencPath}/out/ohlc removed"
mkdir ${krakencPath}/out/ohlc
echo "${krakencPath}/out/ohlc mkdir"
rm ${krakencPath}/out/trades/today/*.csv
echo "${krakencPath}/out/trades/today/*.csv removed"
rm ${krakencPath}out/trades/today
echo "${krakencPath}/out/trades/today removed"
mkdir ${krakencPath}/out/trades/today
echo "${krakencPath}/out/trades/today mkdir"

node ${krakencPath}/main-get-t-today-a.js \{\"asset\":\"XBT\",\"currency\":\"EUR\",\"n\":1000\}
echo "main-get-t-today-a.js for XBT EUR done"
node ${krakencPath}/main-get-o-a.js \{\"asset\":\"XBT\",\"currency\":\"EUR\"}
echo "main-get-o-a.js for XBT EUR done"

rm -r ${dataPath}/trades/xbt-today-$today
echo "${dataPath}/trades/xbt-today-$today removed"
mkdir ${dataPath}/trades/xbt-today-$today
echo "${dataPath}/trades/xbt-today-$today maked"

mv -n ${krakencPath}/out/trades/today/*.csv ${dataPath}/trades/xbt-today-$today
echo "${krakencPath}/out/trades/today/*.csv moved to ${dataPath}/trades/xbt-today-$today"
mkdir ${dataPath}/ohlc/xbt/$now
mv ${krakencPath}/out/ohlc/*.csv ${dataPath}/ohlc/xbt/$now
echo "${krakencPath}/out/ohlc/*.csv moved to ${dataPath}/ohlc/xbt/$now"

sudo rm -rf ${dataPath}/parquets/XBT/EUR/TRADES/today/*
echo "${dataPath}/parquets/XBT/EUR/TRADES/today/* removed"
mkdir ${dataPath}/parquets/XBT/EUR/TRADES/today/parquet
echo "{dataPath}/parquets/XBT/EUR/TRADES/today/parquet maked"

echo "Starting To parquets with spark for ohlc xbt..."
~/pack/bin/cryptos-apps to-parquets-from-csv --master local --api ohlc --input-dir ${dataPath}/ohlc/xbt/$now --parquets-dir ${dataPath}/parquets --minimum 500
echo "To parquets with spark for ohlc xbt done"

echo "Starting To parquets with spark for trades xbt-today-$today..."
~/pack/bin/cryptos-apps to-parquets-from-today-csv --master local --api trades --input-dir ${dataPath}/trades/xbt-today-$today --parquets-dir ${dataPath}/parquets --minimum 1
echo "To parquets with spark for trades xbt-today-$today done"

echo "Get kraken successfully completed"

echo "Starting Prediction ..."
~/pack/bin/cryptos-apps prediction
echo "Prediction done"
