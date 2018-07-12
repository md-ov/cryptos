require('./global.js');console.log(globalvar);const krakenc = krakenclient;

const fs = require('fs');const dateFormat = require('dateformat');

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
	a = await krakenc.api('OHLC', { pair : 'XXLMZEUR', interval  : 240, since : 0 });

	// fs.writeFile('aaa.csv', result.XXBTZEUR, 'utf8', function (err) {
  // if (err) {
  //   console.log('Some error occured - file either not saved or corrupted file saved.');
  // } else{
  //   console.log('It\'s saved!');
  // }
	// });

})();
