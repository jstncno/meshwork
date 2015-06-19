var http = require('http');
var assert = require('assert');
var hbase = require('hbase');

var host = 'ec2-52-8-87-99.us-west-1.compute.amazonaws.com';
//var host = '52.8.87.99';
console.log(host)
http.createServer(function (req, res) {
    res.writeHead(200, {'Content-Type': 'text/plain'});
    res.write('Hello World\n');
    var row = websitesTable.row('google.com')
    row.get('Data:VertexId', function(error, value){
        res.write(value[0].column.toString());
        res.write(value[0]['$'].toString());
        //res.end(value[0]['$'].readUInt32BE(0));
        //res.end(value[0]['$'].readUInt32LE(0));
        res.write('\n');
    });

    row.get('Data:PageRank', function(error, value){
        res.write(value[0].column.toString());
        res.write(value[0]['$'].toString());
        //res.end(value[0]['$'].readDoubleBE(0));
        //res.end(value[0]['$'].readDoubleLE(0));
        res.write('\n');
    });

    row.get('Data:URL', function(error, value){
        res.write(value[0].column.toString());
        res.write(value[0]['$'].toString());
        res.write('\n');
    });
}).listen(1337, host);
console.log('Server running at http://'+host+':1337/');

var dbPort = 10001;
var hbaseClient = new hbase.Client({ host: '52.8.87.99', port: dbPort });
var websitesTable = new hbase.Table(hbaseClient, 'websites');

/*
websitesTable.schema(function(error, schema){
  console.log(schema);
});
*/

