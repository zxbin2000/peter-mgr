
var peter = require('../index').getManager();
var Thenjs = require('thenjs');

Thenjs(function(cont) {
    peter.bindDb('mongodb://localhost:27017/test', cont);
}).then(function(cont, args) {
    peter.count('@Customer', { state: 40 }, cont);
}).then(function(cont, args) {
    console.log(args);
}).fail(function(cont, error) {
    console.log('error', error);
});