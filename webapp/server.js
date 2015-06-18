var http = require('http');
var assert = require('assert');
var hbase = require('hbase');

var host = 'ec2-52-8-87-99.us-west-1.compute.amazonaws.com';
//var host = '52.8.87.99';
console.log(host)
http.createServer(function (req, res) {
  res.writeHead(200, {'Content-Type': 'text/plain'});
  res.end('Hello World\n');
}).listen(1337, host);
console.log('Server running at http://'+host+':1337/');

var dbPort = 10001;
var hbaseClient = new hbase.Client({ host: '52.8.87.99', port: dbPort });
var websitesTable = new hbase.Table(hbaseClient, 'websites');

console.log(hbaseClient.connection.client);

websitesTable.schema(function(error, schema){
  console.log(schema);
});

