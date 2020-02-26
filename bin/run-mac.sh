#!/bin/bash
now="$(date +'%Y-%m-%d')"

#mac
krakencNode="/Users/minhdungdao/ws/git/cryptos/krakenc/"
krakencPath="~/ws/git/cryptos/krakenc/"
dataPath="~/ws/data/cryptos/"

cd $krakencPath

if ! [ -f ${dataPath}trades\\xbt-before-$now ]; then
	echo "Folder ${dataPath}trades\\xbt-before-$now does not exist"
	echo "Retrieve trades from last timestamp BEGIN"
	rm ${krakencPath}out/trades/*.csv
	rm ${krakencPath}out/trades/
	mkdir ${krakencPath}out/trades/
	echo "${krakencPath}out/trades/*.csv and ${krakencPath}out/trades/ removed, ${krakencPath}out/trades/ created"

	node ${krakencNode}main-get-t-a.js \{\"asset\":\"XBT\",\"currency\":\"EUR\",\"n\":1000\}
	echo "main-get-t-a.js for XBT EUR done"

	mkdir ${dataPath}trades/xbt-before-$now/
	echo "${dataPath}trades/xbt-before-$now/ maked"

	mv -n ${krakencPath}out/trades/*.csv ${dataPath}trades/xbt-before-$now/
	echo "krakenc/out/trades/*.csv moved to ${dataPath}trades/xbt-before-$now/"
	
	echo "Retrieve trades from last timestamp DONE"

	echo "cryptos-apps to-parquets-from-csv trades"
	~/pack/bin/cryptos-apps to-parquets-from-csv --master local --api trades --input-dir ${dataPath}trades/xbt-before-$now --parquets-dir ${dataPath}parquets --minimum 1
	echo "cryptos-apps to-parquets-from-csv trades for xbt-before-$now done"
fi
