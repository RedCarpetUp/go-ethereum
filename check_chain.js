const Web3 = require('web3');
const XMLHttpRequest = require("xmlhttprequest").XMLHttpRequest;

let web3 = new Web3();
web3.setProvider(new web3.providers.HttpProvider('http://localhost:8545'));

var ethScanApiUrlPart = 'https://ropsten.etherscan.io/api?module=proxy&action=eth_getBlockByNumber&boolean=true&apikey=DICY1JHH4ZEDTRQ16F7GJMSSEKJX64TW3H&tag='

function httpGet(theUrl){
	var xmlHttp = new XMLHttpRequest();
	xmlHttp.open( "GET", theUrl, false ); // false for synchronous request
	xmlHttp.send( null );
	return xmlHttp.responseText;
}

var promise_block_number = web3.eth.getBlockNumber()
promise_block_number.then(function(block_number){
	web3.eth.getBlock(block_number).then(a=>{
		hash_geth=a.hash;
		var hash_hex = web3.utils.toHex(block_number)
		var ethScanApiUrl = ethScanApiUrlPart+hash_hex
		var req_result = httpGet(ethScanApiUrl)
		json_req = JSON.parse(req_result)
		var hash_ethscan = json_req.result.hash
		if (hash_ethscan==hash_geth){
			console.log("true")
		}else{
			console.log("false")
		} 
		console.log(hash_ethscan)
		console.log(hash_geth)
		console.log(block_number);
	})
})

