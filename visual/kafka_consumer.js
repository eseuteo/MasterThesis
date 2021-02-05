/**
 A Flink Job sink data into Kafka, using the topic: dwddata
 this script:
 1. makes the connection to Kafka and consume data from that topic: dwddata
 2. create a WebSocketServer, so the webSocketInvoke() script in the web page can read from the websocket

 Run with (other settings are hard coded):
   node kafka_consumer.js dwddata

 on another consola run:
  firefox StreamingVisualizationJob.html

*/
var kafka_topic = process.argv[2];
console.log("Reading from Kafka topic: " + kafka_topic);


var WebSocketServer = require('websocket').server;
var http = require('http');
 
var kafka = require('kafka-node');
var Consumer = kafka.Consumer,
 client = new kafka.KafkaClient("localhost:9092"),
 consumer = new Consumer(
 client, [ { topic: kafka_topic, partition: 0 } ], { autoCommit: false });
 
var server = http.createServer(function(request, response) {
 console.log(' Request recieved : ' + request.url);
 response.writeHead(404);
 response.end();
});
server.listen(8080, function() {
 console.log('Listening on port : 8080');
});
 
webSocketServer = new WebSocketServer({
 httpServer: server,
 autoAcceptConnections: false
});
 
function iSOriginAllowed(origin) {
 return true;
}
 
webSocketServer.on('request', function(request) {
 if (!iSOriginAllowed(request.origin)) {
 request.reject();
 console.log(' Connection from : ' + request.origin + ' rejected.');
     return;
}
 
 var connection = request.accept('echo-protocol', request.origin);
 console.log(' Connection accepted : ' + request.origin);
 connection.on('message', function(message) {
 if (message.type === 'utf8') {
 console.log('Received Message: ' + message.utf8Data);
 }
 });
 consumer.on('message', function (message) {
 console.log(message);
 connection.sendUTF(message.value);
 });
 connection.on('close', function(reasonCode, description) {
 console.log('Connection ' + connection.remoteAddress + ' disconnected.');
 });
});     
