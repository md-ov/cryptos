require('./global.js');const krakenc = krakenclient;
var argvJson = {}; try {  argvJson = JSON.parse(process.argv[2]);
} catch(error) { console.error("not a valid json argument"); }

(async () => {
    if 	(argvJson.program == 'balance') {
    	console.log("Display user's balance");
	    console.log(await krakenc.api('Balance'));

	}

	else if (argvJson.program == 't') {
	    console.log(await krakenc.api('Assets', {asset : 'BCH, XBT'}));

	}

	else if (argvJson.program == 'tr') {
      	a = await krakenc.api('Trades', { pair : 'XXBTZEUR', since : 0 });
      	result = a.result
      	console.log(result.last)
      	console.log(result.XXBTZEUR[0])

    }


    else if (argvJson.program == 'opens') {
        a = await krakenc.api('OpenOrders', { trades : false});
        opens = a.result.open
        openOdersKeys = Object.keys(opens)
        openOdersKey = openOdersKeys[argvJson.i]
        console.log(openOdersKey)
        console.log(opens[openOdersKey])

    }

    else if (argvJson.program == 'cancel') {
        console.log("Cancel an order with txid (transaction id) in parameters : " + argvJson.txid);
        a = await krakenc.api('CancelOrder', { txid : argvJson.txid});

    }

    else if (argvJson.program == 'buy') {
        console.log("Place an order to buy BCH with limit price and volume in parameters");
        a = await krakenc.api('AddOrder', { pair: 'BCHEUR', type: 'buy', ordertype: 'limit',
            price: argvJson.price,
            volume : argvJson.vol
        });

    }

    else if (argvJson.program == 'closeds') {
        a = await krakenc.api('ClosedOrders');
        closeds = a.result.closed
        closedOdersKeys = Object.keys(closeds)
        closedOdersKey = closedOdersKeys[argvJson.i]
        console.log(closedOdersKey)
        console.log(closeds[closedOdersKey])

    }

    else if (argvJson.program == 'testsbt') {
      const exec = require("child_process").exec
      
      function parseResponseFromSbt(response) {
          var fields = response.split(',');
          var fieldObject = {};
          
          if (typeof fields === 'object') {
             fields.forEach(function(field) {
                var c = field.trim().split('|');
                fieldObject[c[0]] = c[1];
             });
          }
          return fieldObject;
      }
      
      function puts(error, stdout, stderr) {
        console.log(stderr)
        console.log(stdout)
        console.log(parseResponseFromSbt(stdout))
      }
      exec("cryptos-apps predict --dt 20180808T16:56", puts)
      exec("cryptos-apps parquet-from-csv --csvpath adf --parquet-path ppp", puts)
    }

    else {
	    console.log("Please enter a valid program name for example : {\"program\":\"balance\"}")
	  }



})();
