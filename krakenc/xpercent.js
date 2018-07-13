require('./global.js');const krakenc = krakenclient;
var argvJson = {}; try {  argvJson = JSON.parse(process.argv[2]);
} catch(error) { console.error("not a valid json argument"); }
const g = argvJson.x;
const volNumber = argvJson.vol;

console.log("do you want to spend " + argvJson.vol + " BCH and gain " + g * volNumber / 100 + " BCH in euro ?")
console.log("type \"yes i want\" to execute")
console.log("type \"balance\" to know your balance")
console.log("type \"price\" to know the open price")
console.log("type \"cancel\" to cancel two oders")
console.log("type \"exit\" to exit")

var txid1
var txid2
var stdin = process.openStdin();
stdin.addListener("data", function(d) {
    var entered = d.toString().trim() // note:  d is an object, and when converted to a string it will end with a linefeed.  so we (rather crudely) account for that with toString() and then trim()
    // console.log("you entered: [" + entered + "]");
    if (entered == "exit") process.exit()
    if (entered == "balance") {
        (async () => {
            balance = await krakenc.api('Balance');
            console.log("Your balance is : " + balance.result.ZEUR + "€")
        })();
    }
    if (entered == "price") {
        (async () => {
            ohlc = await krakenc.api('OHLC', { pair : 'BCHEUR', since : 0 });
            list = ohlc.result.BCHEUR
            nowdata = list[list.length-1]
            p = new Number(nowdata[1])
            console.log("open price is : " + p)
        })();
    }
    if (entered == "cancel") {
        (async () => {
            await krakenc.api('CancelOrder', { txid : txid1});
            await krakenc.api('CancelOrder', { txid : txid2});
        })();
        console.log("Oders are canceled")
    }
    if (entered == "yes i want") {
        const mf = 0.16/100;
        const a = argvJson.x/2 + 0.16;
        const p1p = (100 - a) / 100;
        const p2p = (100 + a) / 100;
        (async () => {
            balance = await krakenc.api('Balance');
            eurobalance = new Number(balance.result.ZEUR)
            console.log("Your balance is : " + eurobalance + "€")

            ohlc = await krakenc.api('OHLC', { pair : 'BCHEUR', since : 0 });
            list = ohlc.result.BCHEUR
            nowdata = list[list.length-1]
            p = new Number(nowdata[1])
            console.log("open price is : " + p)

            price1 = Math.floor(p1p*p * 10) / 10
            price2 = Math.floor(p2p*p * 10) / 10

            oder1 = await krakenc.api('AddOrder', { pair: 'BCHEUR', type: 'buy', ordertype: 'limit',
                price: price1,
                volume : volNumber
            });
            txid1 = oder1.result.txid[0]
            oder2 = await krakenc.api('AddOrder', { pair: 'BCHEUR', type: 'sell', ordertype: 'limit',
                price: price2,
                volume : volNumber
            });
            txid2 = oder2.result.txid[0]

            console.log("Orders placed : " + price1 + " " + price2)
            console.log("Your future balance would be : " + (eurobalance + g*p*volNumber/100) + "€")
        })();
    }
});
