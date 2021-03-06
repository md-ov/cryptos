require('./global.js');console.log(globalvar);const krakenc = krakenclient;

const fs = require('fs');const dateFormat = require('dateformat');

(async () => {
    a = await krakenc.api('OHLC', { pair : 'XXLMZEUR', interval  : 30, since : 0 });
	var file = fs.createWriteStream('./out/xlmeur.csv');
	file.on('error', function(err) { console.log("error createWriteStream ") });
	a.result.XXLMZEUR.forEach(function(v) {
		var dt = dateFormat(new Date(v[0]*1000), dateTimeFormat);
		file.write(coins.XLM + csvSeparator + 'EUR' + csvSeparator + 'kraken' + csvSeparator + 'OHLC'
		+ csvSeparator + dt + csvSeparator + v.join(csvSeparator) + returnCharacter);
	});
	file.end();
})();
