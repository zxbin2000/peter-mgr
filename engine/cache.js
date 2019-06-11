var assert = require('assert');
var sprintf = require("sprintf-js").sprintf;
var utils = require('../utils/utils');
var Lock = require('./lock');
var Engine = require('./engine');

function Cache(name, ttl, loader, verbose) {
    this.name = name;
    this.ttl = ttl;
    this.all = {};              // key -> [lock, result, timer]
    this.loader = loader;       // loading function
    this.verbose = verbose;
}

var pp = Engine.generate(__dirname + '/cache.pp');
assert(null != pp, "Failed to generate from cache.pp");
eval(pp);

// loader is optional
Cache.prototype.read = function (key, loader, callback) {
    read(this, key, loader, callback);
};

// options: {loader: func, on_succ: func, on_fail: func}
Cache.prototype.read_change = function (key, options, callback) {
    read_change(this, key, options, callback);
};

Cache.prototype.expireNow = function (key, callback) {
    var cache = this.all[key];
    if (!cache) {
        return process.nextTick(callback, 'Invalid arguments', null);
    }
    expire(this, key, cache, callback);
};

Cache.prototype.getAll = function () {
    var out = {};
    for (var x in this.all) {
        out[x] = this.all[x][1];
    }
    return out;
};

module.exports = {
    create: function (name, ttl, loader, verbose) {
        var lof = new Cache(name, ttl, loader, verbose);
        return lof;
    }
};