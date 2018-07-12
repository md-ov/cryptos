global.globalvar = 'Welcome to Kraken Client Program'

var keys = require('./keys.js')
const KrakenClient = require('kraken-api');

global.krakenclient = new KrakenClient(keys.key, keys.secret);