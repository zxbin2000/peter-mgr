let g_all = {};
let peter = require('./manager/peter');

function createManager(key) {
    if (undefined == key || null == key) {
        key = 0;
    }
    if (g_all.hasOwnProperty(key)) {
        console.log('Warning: peter manager "' + key + '" has already been created!');
        return g_all[key];
    }

    let mgr = peter.createManager();
    g_all[key] = mgr;
    return mgr;
}

function getManager(key) {
    if (undefined == key || null == key) {
        key = 0;
    }
    if (!g_all[key]) {
        createManager(key);
    }
    return g_all[key];
}

module.exports = {
    createManager: createManager,
    getManager: getManager,

    Engine: require('./engine/engine'),
    Index: require('./index/index'),
    Schema: require('./manager/schema'),
    Parser: require('./manager/parser'),
    Service: require('./engine/ppsrv'),
    Postman: require('./utils/postman'),
    Concurrent: require('./engine/concurrent'),
    Parallel: require('./engine/parallel'),
    Leadof: require('./engine/leadof'),
    Lock: require('./engine/lock'),
    Cache: require('./engine/cache'),
    Sync: require('./engine/sync'),
    Utils: require('./utils/utils'),
    DataStructure : require('./data_structure/_index'),
    Exec: require('./utils/exec')
};
