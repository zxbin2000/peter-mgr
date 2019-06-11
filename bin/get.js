#!/usr/bin/env node

var peter, index, utils;
try {
    peter = require('./manager/peter').createManager();
    index = require('./index/index');
    utils = require('./utils/utils');
} catch (e) {
    var _peter = require('peter');
    peter = _peter.createManager();
    index = _peter.Index;
    utils = _peter.Utils;
}
var Thenjs = require('thenjs');
var fs = require('fs');
var beautify = require('js-beautify').js_beautify;

var mongoaddr = utils.mongoAddrFromConf();;
var argv = utils.getRealArgv(__filename);

if (argv.length < 2) {
    console.log('get pid [-np] [-o [out.json]]');
    process.exit(-1);
}

var out, pid, bPrint = true;
pid = argv[1];

for (var i=2; i<argv.length; i++) {
    switch (argv[i]) {
        case '-np':
            bPrint = false;
            break;
        case '-o':
            if (i < argv.length - 1) {
                out = argv[++i];
            }
            else {
                out = pid + '.json';
            }
            break;
    }
}
console.log('get %s', pid, out ? '-> '+out : '');

Thenjs(function (cont) {
    console.log("Fetching schemas...");
    peter.bindDb(mongoaddr, cont);
})
.then(function(cont, arg) {
    console.log("Okay, %d schema\nLoading index...", arg);
    peter.get(pid, cont);
})
.then(function(cont, arg) {
    var str = beautify(JSON.stringify(arg));//JSON.stringify(arg, null, 0);
    if (bPrint) {
        console.log(str);
    }
    if (undefined != out) {
        fs.writeFile(out, str, cont);
    }
    else {
        process.exit(0);
    }
})
.then(function(cont, arg) {
    console.log('saved to', out);
    process.exit(0);
})
.fail(function (cont, err, arg) {
    console.error("Error: " + err.stack);
    process.exit(-1);
});
