#!/bin/bash
now="$(date +'%Y-%m-%d')"
echo "Starting To parquets with spark at $now..."
cryptos-apps to-parquets-from-csv --master local --api trades --input-dir D:\\ws\\cryptos\\data\\trades\\xbt-before-$now --parquets-dir file:///D:\\ws\\cryptos\\data\\parquets --minimum 1
echo "to-parquets-from-csv for trades xbt : xbt-before-$now done"

cryptos-apps to-parquets-from-today-csv --master local --api trades --input-dir D:\\ws\\cryptos\\data\\trades\\xbt-today-$now --parquets-dir file:///D:\\ws\\cryptos\\data\\parquets --minimum 1
echo "to-parquets-from-today-csv for trades xbt : xbt-today-$now done"
cmd