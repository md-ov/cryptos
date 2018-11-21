require('./global.js');
var os = require("os");
const krakenc = krakenclient;
const fs = require('fs');
const dateFormat = require('dateformat');
var argvJson = {};
try {
  argvJson = JSON.parse(process.argv[2]);
} catch(error) { console.error("not a valid json argument"); }

var outdir = './out/trades/';
fs.existsSync(outdir) || fs.mkdirSync(outdir);

function getTs(myDate) {
  return new Date(myDate).getTime() * 1000000;
}
var now = new Date();
var maxSince = getTs(now.getFullYear() + "/" + (now.getMonth() + 1) + "/" + now.getDate());
console.log('max since : ' + maxSince);
numberOfIteration = argvJson.n;
asset = argvJson.asset;
currency = argvJson.currency;
pair = getPair(asset, currency);
sinceFile = './metadata/since/trades/' + asset + currency +'/0'
var since = 0;
var lastsince = 0;

(async () => {
    var lineReader = require('line-reader');
    await lineReader.eachLine(sinceFile, function(line, last) {
      if(last){
        since = getLastSince(line);
      }
    });
    await sleep(2)
    var loop = true;
    for (var i = 1; (i <= numberOfIteration) && loop; i++) {
      lastsince = since
      try {
        console.log(since)
        a = await krakenc.api('Trades', { pair : pair, since : since });
        var fileName = outdir + i + '.csv'
        var file = fs.createWriteStream(fileName);
        file.on('error', function(err) { console.log("error createWriteStream ") });
        a.result[pair].forEach(function(v) {
          var tsOfLine = v[2]*1000
      		var dt = dateFormat(new Date(tsOfLine), dateTimeFormat);
          if (tsOfLine*1000000 < maxSince) {
        		file.write(asset + csvSeparator + currency + csvSeparator + 'kraken' + csvSeparator + 'trades'
        		+ csvSeparator + dt + csvSeparator + v.join(csvSeparator) + returnCharacter);
          } else {
            loop = false;
          }
      	});
      	file.end();
        if (a.result.last >= maxSince) {
          loop = false;
        }
        since = Math.min(a.result.last, maxSince);
        console.log(fileName + ' done');
      } catch(error) {
        since = lastsince
        console.error("problem : "+ since);
        await sleep(10)
        console.log("sleep done");
      }
    }

    var sinceF = fs.appendFile(sinceFile,
      since + dataSeparator + dateFormat(new Date(), dateTimeFormat) + os.EOL,
      function (err) {
      if (err) console.log(err);
    });;

})();
