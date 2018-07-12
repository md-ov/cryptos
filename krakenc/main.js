const key          = 'HrJqqxZyt1rttQfp4F7rNe9yqR4cjY542FBDuUHEGvkEEqfx7aUc6yxp'; // API Key
const secret       = 'O321ILCOl/v2gcutIc9vtMKyiTFRtK8Re9znpNxdyKl56qeLOqRQHZ6rfbQoXyjX2WSU7ttBxZq4gYDGI0Y2yA=='; // API Private Key
const KrakenClient = require('kraken-api');
const kraken       = new KrakenClient(key, secret);
const fs = require('fs');
const dateFormat = require('dateformat');

(async () => {
	// Display user's balance
	//console.log(await kraken.api('Balance'));

	//console.log(await kraken.api('Assets', {asset : 'BCH, XBT'}));

	// Get Trades Info
	//a = await kraken.api('Trades', { pair : 'XXBTZEUR', since : 0 });
	//result = a.result
	//console.log(result.last)
	//console.log(result.XXBTZEUR[0])

	// a = await kraken.api('OHLC', { pair : 'XXBTZEUR', interval  : 240, since : 0 });
	// a = await kraken.api('OHLC', { pair : 'BCHEUR', interval  : 240, since : 0 });
	// a = await kraken.api('OHLC', { pair : 'EOSEUR', interval  : 240, since : 0 });
	a = await kraken.api('OHLC', { pair : 'XXLMZEUR', interval  : 240, since : 0 });

	// fs.writeFile('aaa.csv', result.XXBTZEUR, 'utf8', function (err) {
  // if (err) {
  //   console.log('Some error occured - file either not saved or corrupted file saved.');
  // } else{
  //   console.log('It\'s saved!');
  // }
	// });

	var file = fs.createWriteStream('bbb.csv');
	file.on('error', function(err) { console.log("error createWriteStream ") });
	a.result.XXLMZEUR.forEach(function(v) {
		var dt = dateFormat(new Date(v[0]*1000), "yyyy-mm-dd'T'hh:MM:ssZ");
		file.write('XLM;' + dt + ';' + v.join(';') + '\n');
	});
	file.end();
})();
