-- fill key and secret key in keys.js

npm install
node main.js \{\"program\":\"testsbt\"\}
node main.js \{\"program\":\"balance\"\}  (without any space)
node main.js {\"program\":\"balance\"}  (for windows)
node main.js \{\"program\":\"opens\",\"i\":1\}
node main.js \{\"program\":\"buy\",\"price\":\"510\",\"vol\":\"0.1\"\}
node main.js \{\"program\":\"cancel\",\"txid\":\"OAOW2D-E62UU-AVR6XE\"\}
node xpercent.js \{\"vol\":0.1,\"x\":10\}





node mainCleanOut.js
node mainGetXLMEUR.js
node main-get-o.js \{\"asset\":\"XBT\",\"currency\":\"EUR\",\"since\":\"1540373460\"\}
node main-get-o-a.js \{\"asset\":\"XBT\",\"currency\":\"EUR\"}   //automatique get ohlc avec le since persisté
interval : 1 (default), 5, 15, 30, 60, 240, 1440, 10080, 21600

node main-get.js \{\"asset\":\"XLM\",\"currency\":\"EUR\",\"n\":3,\"start\":\"2017-05-08\"\}
node main-get.js \{\"asset\":\"XBT\",\"currency\":\"EUR\",\"n\":10000,\"start\":\"2018-01-10\"\}
start : date to start retrieve elements
each request from kraken will give you 1000 elements, if you want to get 3000 elements n would be 3



node main-get-t-a.js \{\"asset\":\"XBT\",\"currency\":\"EUR\",\"n\":1000\}

interesting pairs:
XXBTZEUR
XXLMZEUR
BCHEUR
EOSEUR
BCHXBT
