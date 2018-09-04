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

global.getPair = function(asset, currency) {
  if (asset.toLowerCase()  == 'xlm' && currency.toLowerCase() == 'eur') return 'XXLMZEUR';
  if (asset.toLowerCase()  == 'xbt' && currency.toLowerCase() == 'eur') return 'XXBTZEUR';
  if (asset.toLowerCase()  == 'bch' && currency.toLowerCase() == 'eur') return 'BCHEUR';
  if (asset.toLowerCase()  == 'eos' && currency.toLowerCase() == 'eur') return 'XXLMZEUR';
  else return '';
}

global.sleep = function(ms) {
  console.log("sleep now for " + ms + " seconds")
  return new Promise(resolve => setTimeout(resolve, ms*1000));
}
