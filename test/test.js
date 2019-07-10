
var peter = require('../index').getManager();
var Thenjs = require('thenjs');

Thenjs(function(cont) {
    peter.bindDb('mongodb://localhost:27017/test', cont);
}).then(function(cont, args) {
    peter.findOne('@User', { is_deleted: false }, cont);
}).then(function(cont, user) {
    peter.findOneAndUpdate('@Follow', {
        user_id: 1
    }, {
        $set: {
            user_id: user._id,
            update_time: new Date()
        }
    }, {
        upsert: true,
        returnOriginal: false
    }, cont);
}).then(function(cont, args) {
    console.log('----', args);
    process.exit(0);
}).catch(function(cont, error) {
    console.log('====', error);
    process.exit(0);
});

// Thenjs(function(cont) {
//     peter.bindDb('mongodb://localhost:27017/test', cont);
// }).then(function(cont, args) {
//     console.log('==connection end==');
//     peter.create('@User', { 
//         nick_name: "test1", 
//         avatar: 'http',
//         gender: 0,
//         role: 0,
//         n_subscribe: 0,
//         n_follower: 0,
//         n_proposal: 0
//     }, cont);
// }).then(function(cont, args) {
//     let uid = args;
//     peter.get(uid, cont);
// }).then(function(cont, args) {
//     peter.query('@User', { nick_name: "test1" }, cont);
// }).then(function(cont, args) {
//     let update = { $set: { avatar: 'update'} };
//     let option = { returnNewDocument: true };
//     peter.findOneAndDelete('@User', { nick_name: 'test' }, cont);
// }).then(function(cont, args) {
//     console.log('success', args);
// }).fail(function(cont, error) {
//     console.log('error', error);
// });