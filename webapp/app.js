var express = require('express');
var app = express();
var hbase = require('hbase');

var dbPort = 10001;
var hbaseClient = new hbase.Client({ host: '52.8.87.99', port: dbPort });
var websitesTable = new hbase.Table(hbaseClient, 'websites');

app.get('/', function (req, res) {
  res.send('Hello World!');
});

var fetchDB = function(req, res, next) {
    var row = websitesTable.row(req.params.key);
    row.get(function(error, value){
        if (error) {
            return next(new Error("Invalid key! " + error));
        }
        data = {}
        req.numItems = value.length.toString();
        data['vertexId'] = value[2]['$'].toString();
        req.vertexId = value[2]['$'].toString();
        data['pageRank'] = value[0]['$'].toString();
        req.pageRank = value[0]['$'].toString();
        data['url'] = value[1]['$'].toString();
        req.url = value[1]['$'].toString();
        console.log(value[0].column.toString());
        console.log(value[0]['$'].toString());
        req.data = data;
        next();
    });
}

app.get('/:key', fetchDB, function(req, res) {
    var data = req.url+'\n'+req.vertexId+'\n'+req.pageRank+'\n';
    res.send(req.data);
    //res.send(req.params.key);
    /*
    row.get('Data:PageRank', function(error, value){
        console.log(value[0].column.toString());
        console.log(value[0]['$'].toString());
        //console.log(value[0]['$'].readDoubleBE(0));
        //console.log(value[0]['$'].readDoubleLE(0));
        console.log('\n');
    });

    row.get('Data:URL', function(error, value){
        console.log(value[0].column.toString());
        console.log(value[0]['$'].toString());
        console.log('\n');
    });
    */
});

var server = app.listen(3000, function () {

  var host = server.address().address;
  var port = server.address().port;

  console.log('Example app listening at http://%s:%s', host, port);

});
