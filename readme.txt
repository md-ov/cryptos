add to path
D:\ws\cryptos\cryptos\sryptosbt\target\pack\bin\

mettre winutils.ext dans D:\ws\haoop\bin et mettre la variable d'environnement
HADOOP_HOME D:\ws\haoop\




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

cryptos-apps to-parquets-from-csv --master local --api trades --input-dir D:\\ws\\cryptos\\data\\trades --parquets-dir file:///D:\\ws\\cryptos\\data\\parquets --minimum 500


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