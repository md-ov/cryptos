require('./global.js');const krakenc = krakenclient;
const fs = require('fs');const dateFormat = require('dateformat');
var argvJson = {}; try {  argvJson = JSON.parse(process.argv[2]);
} catch(error) { console.error("not a valid json argument"); }

function getTsForKraken(myDate) {
  myDate=myDate.split("-");
  var newDate=myDate[1]+"/"+myDate[2]+"/"+myDate[0];
  return new Date(newDate).getTime() * 1000000;
}

(async () => {
    asset = argvJson.asset;
    currency = argvJson.currency;
    pair = getPair(asset, currency);
    intervals = [1, 5, 15, 30, 60, 240, 1440, 10080, 21600];

    for (var i= 0; i < intervals.length; i++) {
      interval = intervals[i];
      try {
        console.log(interval);

        a = await krakenc.api('OHLC', { pair : pair, interval : interval});
        var fileName = './out/' + pair + interval + '.csv'
        var file = fs.createWriteStream(fileName);
        file.on('error', function(err) { console.log("error createWriteStream ") });
        a.result[pair].forEach(function(v) {
      		var dt = dateFormat(new Date(v[0]*1000), dateTimeFormat);
      		file.write(asset + csvSeparator
            + currency + csvSeparator + 'kraken'
            + csvSeparator + 'ohlc'
        		+ csvSeparator + dt + csvSeparator
            + v.join(csvSeparator) + returnCharacter);
      	});
        file.end();
        console.log(fileName + ' done');
      } catch(error) {
        console.error("problem : " + interval);
        await sleep(5);
        console.log("sleep done");
        i = i - 1;
      }
    }
})();
