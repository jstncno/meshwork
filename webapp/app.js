var express = require('express');
var app = express();
var hbase = require('hbase');
var md5 = require('MD5');

var dbPort = 10001;
var hbaseClient = new hbase.Client({ host: '52.8.87.99', port: dbPort });
var websitesTable = new hbase.Table(hbaseClient, 'websitesClone06232015');

app.get('/', function (req, res) {
  res.send('Hello World!');
});

var getRowKey = function(req, res, next) {
    var keyHash = md5(req.params.key);
    var buf = new Buffer(keyHash, 'hex');
    var rowKey = buf.readInt32BE(0).toString();
    req.params.key = rowKey;
    next();
}

var fetchDB = function(req, res, next) {
    var row = websitesTable.row(req.params.key);
    row.get(function(error, record){
        if (error) {
            return next(new Error("Invalid key! " + error));
        }
        var data = {}
        data['Data'] = {}
        data['Neighbors'] = {}

        for (index in record) {
            var value = record[index]['$'].toString();
            var column = record[index]['column'].toString();
            switch (column) {
                case 'Data:PageRank':
                    data['Data']['PageRank'] = value;
                    break;
                case 'Data:URL':
                    data['Data']['URL'] = value;
                    break;
                case 'Data:VertexId':
                    data['Data']['VertexId'] = value;
                    break;
                case 'Neighbors:FirstDegree':
                    data['Neighbors']['FirstDegree'] = value.split(',');
                    break;
                case 'Neighbors:SecondDegree':
                    data['Neighbors']['SecondDegree'] = value.split(',');
                    break;
                default:
                    break;
            }
        }

        req.data = data;
        next();
    });
}
/*
var fetchNeighborNeighbors = function(req, res, next) {
    var firstDegreeNeighbors = req.data['Neighbors']['FirstDegree'];
    for(index in firstDegreeNeighbors) {
        var row = websitesTable.row(firstDegreeNeighbors[index]);
        row.get(function(error, record){
        });
    }
}
*/
app.get('/search/:key', getRowKey, fetchDB, function(req, res) {
    var data = req.url+'\n'+req.vertexId+'\n'+req.pageRank+'\n';
    res.send(req.data);
});

app.get('/id/:key', fetchDB, function(req, res) {
    var data = req.url+'\n'+req.vertexId+'\n'+req.pageRank+'\n';
    res.send(req.data);
});

var server = app.listen(3000, function () {

  var host = server.address().address;
  var port = server.address().port;

  console.log('Example app listening at http://%s:%s', host, port);

});
