
var peter = require('../index').getManager();
var Thenjs = require('thenjs');

Thenjs(function(cont) {
    peter.bindDb('mongodb://localhost:27017/test', cont);
}).then(function(cont, args) {
    console.log('==connection end==');
    peter.create('@User', { name: "test" }, cont);
}).then(function(cont, args) {
    console.log('==test create end==', args);
    peter.count('@User', cont);
}).then(function(cont, args) {
  console.log('==test count end==', args);
}).fail(function(cont, error) {
    console.log('error', error);
});