add to path
D:\ws\cryptos\cryptos\sryptosbt\target\pack\bin\

mettre winutils.ext dans D:\ws\haoop\bin et mettre la variable d'environnement
HADOOP_HOME D:\ws\haoop\




use of cryptos apps : 
to genenerate parquet from csv : 
cryptos-apps parquet-from-csv --master local --csvpath D:\ws\cryptos\data\v3\xlmeur.csv --parquet-path D:\ws\cryptos\data\parquets\parquet

to genenerate csv from parquet (il will make a.csv)
cryptos-apps csv-from-parquet --master local --csvpath D:\ws\cryptos\data\fromparquet\a --parquet-path file:///D:\ws\cryptos\data\parquets\parquet

to integrate csv to parquets system
cryptos-apps to-parquets-from-csv --master local 
--csvpath /home/mdao/minh/git/cryptos/krakenc/out/xlmeur.csv 
--parquets-dir file:///home/mdao/minh/git/cryptos/data/parquets
--partition-element-number 6
