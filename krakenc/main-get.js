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
    var since = getTsForKraken(argvJson.start).toString();
    var lastsince = since
    for (var i = 1; i <= argvJson.n; i++) {
      lastsince = since
      try {
        console.log(since)
        a = await krakenc.api('Trades', { pair : pair, since : since });
        var fileName = './out/trades/' + i + '.csv'
        var file = fs.createWriteStream(fileName);
        file.on('error', function(err) { console.log("error createWriteStream ") });
        a.result[pair].forEach(function(v) {
      		var dt = dateFormat(new Date(v[2]*1000), dateTimeFormat);
      		file.write(asset + csvSeparator + currency + csvSeparator + 'kraken' + csvSeparator + 'trades'
      		+ csvSeparator + dt + csvSeparator + v.join(csvSeparator) + returnCharacter);
      	});
      	file.end();
        since = a.result.last;
        console.log(fileName + ' done');
      } catch(error) {
        since = lastsince
        console.error("problem : "+ since);
        await sleep(10)
        console.log("sleep done")
      }
    }
})();
