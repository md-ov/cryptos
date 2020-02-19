add to path
D:\ws\cryptos\cryptos\sryptosbt\target\pack\bin\

mettre winutils.ext dans D:\ws\haoop\bin et mettre la variable d'environnement
HADOOP_HOME D:\ws\haoop\

va dans krakenc et 
npm install

sbt pack


use of cryptos apps : 
to genenerate parquet from csv : 
cryptos-apps parquet-from-csv --master local --api ohlc --csvpath D:\ws\cryptos\data\v4\xlmeur.csv --parquet-path D:\ws\cryptos\data\parquets\parquet 
//api : trades or ohlc

//for gitbash
cryptos-apps parquet-from-csv --master local --csvpath file:///D:\\ws\\cryptos\\data\\v4\\xlmeur.csv --api ohlc --parquet-path D:\\ws\\cryptos\\data\\parquets\\parquet   




to genenerate csv from parquet (il will make a.csv)
cryptos-apps csv-from-parquet --master local --csvpath D:\ws\cryptos\data\fromparquet\a --parquet-path file:///D:\ws\cryptos\data\parquets\parquet

to integrate csv to parquets system
cryptos-apps to-parquets-from-csv --master local 
--api trades
--csvpath /home/mdao/minh/git/cryptos/krakenc/out/xlmeur.csv 
--parquets-dir file:///home/mdao/minh/git/cryptos/data/parquets
--partition-element-number 6



cryptos-apps to-parquets-from-csv 
--master local 
--api trades 
--input-dir D:\\ws\\cryptos\\data\\trades 
--parquets-dir file:///D:\\ws\\cryptos\\data\\parquets 
--minimum 500

cryptos-apps to-parquets-from-csv --master local --api trades --input-dir D:\\ws\\cryptos\\data\\trades\\xbt-until-17-12-01 --parquets-dir file:///D:\\ws\\cryptos\\data\\parquets --minimum 1

cryptos-apps to-parquets-from-csv --master local --api ohlc --input-dir D:\\ws\\cryptos\\data\\ohlc\\xbt --parquets-dir file:///D:\\ws\\cryptos\\data\\parquets --minimum 500

cryptos-apps extract-to-csv 
--master local 
--parquets-dir file:///D:\\ws\\cryptos\\data\\parquets
--asset xlm
--currency eur
--csvpath D:\\ws\\cryptos\\data\\fromparquet\\extract
--start-day 2017-05-02
--end-day 2017-05-06

cryptos-apps extract-to-csv --master local --parquets-dir file:///D:\\ws\\cryptos\\data\\parquets --asset xlm --currency eur --csvpath D:\\ws\\cryptos\\data\\fromparquet\\extract --start-day 2017-05-02 --end-day 2017-05-06

cryptos-apps sampler --master local --asset xlm --currency eur --delta 20 --parquets-dir file:///D:\\ws\\cryptos\\data\\parquets --csvpath D:\\ws\\cryptos\\data\\fromparquet\\sampling



2018-11-15
la donnée est mal présentée dans le fichiers brut (ohlc, trades) à cause du formatter dans global.js
mais bien parsé par integrateur de parquet en utilisant le timestamp





sh get-kraken.sh