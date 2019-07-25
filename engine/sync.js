let assert = require('assert');
let Postman = require('../utils/postman');
let URL = require('url');
let utils = require('../utils/utils');

// options: {ttl: ttl, timeout: timeout, updater: func, from: from, nocache: true/false}
function Sync(url, options) {
    options = options || {};
    let o = URL.parse(url);
    this.nc = Postman(o.protocol+'//'+ o.host, {mute: options.mute, timeout: options.timeout});
    this.path = o.path;
    this.started = false;
    this.options = options;
    options.ttl = options.ttl ? utils.translateIntervalString(options.ttl) : 60000*5;
    if (options.ttl < 10000)
        options.ttl = 10000;
}

Sync.prototype.start = function (from, callback) {
    let self = this;
    let updater = self.options.updater;
    if ('function' == typeof from) {
        callback = from;
        from = undefined;
    }
    if (!from) {
        from = self.options.from;
    }

    function _do(cb) {
        function addQueryArgs(path, name, arg) {
            let has_query = path.includes('\?');
            return arg ? path+(has_query?'&':'?')+name+'='+arg : path;
        }
        self.nc.get(addQueryArgs(self.path, 'from', from), function (err, arg) {
            if (err == null) {
                if (!self.options.nocache) {
                    self.cache = arg;
                }
                if (updater) {
                    return updater(arg, cb);
                }
            }
            cb(err, arg);
        });
    }

    if (self.started) {
        return !self.options.nocache
            ? process.nextTick(function () {
                return updater
                    ? updater(self.cache, callback)
                    : callback(null, self.cache);
            })
            : _do(callback);
    }
    self.started = true;
    utils.runThenRepeat(_do, self.options.ttl, callback);
};

Sync.prototype.startAlways = function (from, default_value, callback) {
    let self = this;

    if ('function' == typeof from) {
        callback = from;
        from = undefined;
        default_value = {};
    }
    else if ('function' == typeof default_value) {
        callback = default_value;
        default_value = {};
    }

    self.start(from, function (err, arg) {
        if (!err) {
            return callback(err, arg);
        }
        self.cache = default_value;
        callback(null, default_value);
    });
};

module.exports = function (url, options) {
    return new Sync(url, options);
};
