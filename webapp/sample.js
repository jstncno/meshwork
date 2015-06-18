  var assert = require('assert');
  var hbase = require('hbase');
  
  hbase({ host: '127.0.0.1', port: 10001 })
  .table( 'websites' )
  .schema(function(error, schema){
    console.log(schema);
  });
