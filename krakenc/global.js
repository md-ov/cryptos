global.globalvar = 'Welcome to Kraken Client Program'

var keys = require('./keys.js')
const KrakenClient = require('kraken-api');

global.krakenclient = new KrakenClient(keys.key, keys.secret);

global.dateTimeFormat = "yyyy-mm-dd'T'hh:MM:ssZ"

var coins = {}
coins.XBT = "XBT"
coins.BTC = "XBT"
coins.XLM = "XLM"
coins.BCH = "BCH"
coins.BCC = "BCC"
coins.BCH = "BCH"

global.coins = coins
global.csvSeparator = ";"
global.returnCharacter = "\n"

