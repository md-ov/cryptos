require('./global.js');
var os = require("os");
const krakenc = krakenclient;
const fs = require('fs');const dateFormat = require('dateformat');
var argvJson = {}; try {  argvJson = JSON.parse(process.argv[2]);
} catch(error) { console.error("not a valid json argument"); }

asset = argvJson.asset;
currency = argvJson.currency;
pair = getPair(asset, currency);
intervals = [1, 5, 15, 30, 60, 240, 1440, 10080, 21600];
sinceFile = './metadata/since/ohlc/' + asset + currency +'/0'
console.log(sinceFile)
var since = 0;
var lastSince = 0;
(async () => {
  var lineReader = require('line-reader');
  await lineReader.eachLine(sinceFile, function(line, last) {
    if(last){
      since = getLastSince(line);
    }
  });
  await sleep(2)
  for (var i= 0; i < intervals.length; i++) {
    interval = intervals[i];
    try {
      console.log(interval);
      a = await krakenc.api('OHLC', { pair : pair, interval : interval, since : since});
      var fileName = './out/ohlc/' + pair + since + interval + '.csv'
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

      if (i == 0) {
        lastSince = a.result.last
      }

      console.log(fileName + ' done');
    } catch(error) {
      console.error("problem : " + interval);
      await sleep(5);
      console.log("sleep done");
      i = i - 1;
    }
  }

  console.log("last since : " + lastSince)
  var sinceF = fs.appendFile(sinceFile,
    lastSince + dataSeparator + dateFormat(new Date(), dateTimeFormat) + os.EOL,
    function (err) {
    if (err) console.log(err);
  });;



})();
