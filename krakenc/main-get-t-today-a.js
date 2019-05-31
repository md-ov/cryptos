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
var todaySince = getTs(now.getFullYear() + "/" + (now.getMonth() + 1) + "/" + now.getDate());
var nowSince = getTs(now);
numberOfIteration = argvJson.n;
asset = argvJson.asset;
currency = argvJson.currency;
pair = getPair(asset, currency);
var since = todaySince;
var lastsince = since;

(async () => {
    var loop = true;
    for (var i = 1; (i <= numberOfIteration) && loop; i++) {
      lastsince = since;
      try {
        console.log(since)
        console.log("nowSince : " + nowSince)
        a = await krakenc.api('Trades', { pair : pair, since : since });
        var todayFileName = outdir + 'today/'+i +'.csv'
        var todayFile = fs.createWriteStream(todayFileName);
        todayFile.on('error', function(err) { console.log("error createWriteStream on today file") });
        a.result[pair].forEach(function(v) {
          var tsOfLine = v[2]*1000
      		var dt = dateFormat(new Date(tsOfLine), dateTimeFormat);
          todayFile.write(asset + csvSeparator + currency + csvSeparator + 'kraken' + csvSeparator + 'trades'
          + csvSeparator + dt + csvSeparator + v.join(csvSeparator) + returnCharacter);
      	});
        todayFile.end()
        if (a.result.last >= nowSince) {
          loop = false;
        }
        since = a.result.last;
        console.log(todayFileName + ' done');
      } catch(error) {
        since = lastsince
        console.error("problem : " + todayFileName + " - " + since);
        await sleep(10)
        console.log("sleep done");
      }
    }
})();
