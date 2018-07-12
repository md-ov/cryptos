require('./global.js');const krakenc = krakenclient;
const fs = require('fs');const dateFormat = require('dateformat');
var argvJson = {}; try {  argvJson = JSON.parse(process.argv[2]);
} catch(error) { console.error("not a valid json argument"); }
    
(async () => {
    if 	(argvJson.program == 'balance') {
    	console.log("Display user's balance");
	    console.log(await krakenc.api('Balance'));
	    
	} else if (argvJson.program == 't') {
	    console.log(await krakenc.api('Assets', {asset : 'BCH, XBT'}));
	    
	} else if (argvJson.program == 'tr') {
      	a = await krakenc.api('Trades', { pair : 'XXBTZEUR', since : 0 });
      	result = a.result
      	console.log(result.last)
      	console.log(result.XXBTZEUR[0])
    } else {
	    console.log("Please enter a program name for example : {\"program\":\"balance\"}")
	}

})();
