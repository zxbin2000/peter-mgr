#!/usr/bin/env node

var utils, postman;
try {
    utils = require('../utils/utils');
    postman = require('../utils/postman');
} catch (e) {
    var _peter = require('peter');
    utils = _peter.Utils;
    postman = _peter.Postman;
}

var path = require('path');
var URL = require('url');

if (process.argv.length < 3) {
    console.log("node postman url [json_file]");
    process.exit(-1);
}

var url = process.argv[2];
var json = null;
if (process.argv.length > 3) {
    json = utils.loadJsonFile(path.resolve(process.cwd(), process.argv[3]));
}
var o = URL.parse(url);
var nc = postman(o.protocol+'//'+ o.host);


function onRet(err, res) {
    if (null != err) {
        console.log(err);
    }
    else {
        console.log(res);
    }
    process.exit(0);
}

if (null != json) {
    console.log('Post', url, json);
    nc.post(o.path, json, onRet);
}
else {
    console.log('Get', url);
    nc.get(o.path, onRet);
}
