add to path
D:\ws\cryptos\cryptos\sryptosbt\target\pack\bin\

mettre winutils.ext dans D:\ws\haoop\bin et mettre la variable d'environnement
HADOOP_HOME D:\ws\haoop\




use of cryptos apps : 
to genenerate parquet from csv : 
cryptos-apps parquet-from-csv --master local --csvpath D:\ws\cryptos\data\v2\BCH.csv --parquet-path D:\ws\cryptos\data\parquets\bchv2

to genenerate csv from parquet (il will make a.csv)
cryptos-apps csv-from-parquet --master local --csvpath D:\ws\cryptos\data\fromparquet\a --parquet-path file:///D:\ws\cryptos\data\parquets\bchv2
