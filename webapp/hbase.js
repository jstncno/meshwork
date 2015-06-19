var hbase = require('hbase');

var dbPort = 10001;
var hbaseClient = new hbase.Client({ host: '52.8.87.99', port: dbPort });
var websitesTable = new hbase.Table(hbaseClient, 'websites');

/*
websitesTable.schema(function(error, schema){
  console.log(schema);
});
*/

var row = websitesTable.row('google.com')
var data = row.get('Data:VertexId', function(error, value){
    console.log(value[0].column.toString());
    console.log(value[0]['$'].toString());
    //console.log(value[0]['$'].readUInt32BE(0));
    //console.log(value[0]['$'].readUInt32LE(0));
    console.log('\n');
});

console.log(data);

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
