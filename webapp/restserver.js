var express = require('express');
var app = express();
var hbase = require('hbase');
var md5 = require('MD5');

var dbPort = 10001;
var hbaseClient = new hbase.Client({ host: '52.8.106.198', port: dbPort });
var websitesTable = new hbase.Table(hbaseClient, 'websites');

app.get('/', function (req, res) {
  res.send('Hello World!');
});

var getRowKey = function(req, res, next) {
    var query, rowKey;
    query = req.query.id;
    if(query) {
        rowKey = query;
    } else {
        query = req.query.url;
        var keyHash = md5(query);
        var buf = new Buffer(keyHash, 'hex');
        rowKey = buf.readInt32BE(0).toString();
    }
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
        data['Top50Neighbors'] = {}

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
                case 'Top50Neighbors:FirstDegree':
                    data['Top50Neighbors']['FirstDegree'] = value.split(',');
                    break;
                case 'Top50Neighbors:SecondDegree':
                    data['Top50Neighbors']['SecondDegree'] = value.split(',');
                    break;
                default:
                    break;
            }
        }
        req.data = [data];
        next();
    });
}

var setHeaders = function(req, res, next) {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE');
    res.header('Access-Control-Allow-Headers', 'Content-Type');
    res.header('Content-Type', 'application/json');
    next();
}

app.get('/search', getRowKey, fetchDB, setHeaders, function(req, res) {
    res.send(JSON.stringify(req.data));
});

app.get('/data', getRowKey, fetchDB, setHeaders, function(req, res) {
    res.send(JSON.stringify(req.data[0]['Data']));
});

var server = app.listen(3000, function () {

    //var host = server.address().address;
    var host = 'ec2-52-8-106-198.us-west-1.compute.amazonaws.com';
    var port = server.address().port;

    console.log('Example app listening at http://%s:%s', host, port);

});
