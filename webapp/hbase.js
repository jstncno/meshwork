var hbase = require('hbase');

var dbPort = 10001;
var hbaseClient = new hbase.Client({ host: '52.8.87.99', port: dbPort });
var websitesTable = new hbase.Table(hbaseClient, 'websites');

/*
websitesTable.schema(function(error, schema){
  console.log(schema);
});
*/

var row = websitesTable.row('row1')
row.get('Data:URL', function(error, value){
    console.log(value);
    console.log(value[0].column.toString());
    console.log(value[0]['$'].toString());
});
