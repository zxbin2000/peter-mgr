var assert = require('assert');
var sprintf = require("sprintf-js").sprintf;
var utils = require('../utils/utils');

function Leadof(name, ttl) {
    this.name = name;
    this.waiting = [];
    this.ttl = (ttl!==undefined) ? utils.translateIntervalString(ttl) : ttl;
}

function onTimeout(self) {
    self.cache = undefined;
    self.loading = undefined;
    self.timer = undefined;
//    console.log('leadof onTimeout', n, self.name)
}

function extendTimer(self) {
    if (self.ttl<0 && self.timer) {
        clearTimeout(self.timer);
        self.timer = setTimeout(onTimeout, Math.abs(self.ttl), self);
    }
}

// Args: func[, args...], callback
Leadof.prototype.run = function () {
    assert(arguments.length >= 2);
    var args = Array.prototype.slice.call(arguments);
    var func = args.shift();
    var callback = args.pop();
    assert('function' == typeof callback, 'callback not function: '+callback);
    var self = this;

    if (false === self.loading) {
        extendTimer(self);
        return process.nextTick(callback, null, self.cache);
    }
    if (true === self.loading) {
        return self.waiting.push(callback);
    }
    // else should be undefined
    self.loading = true;

    args.push(function (err, arg) {
//        console.log('onret', err);
        if (null == err) {
            switch (self.ttl) {
            case undefined:
                self.cache = arg;
                self.loading = false;
                break;

            case 0:
                self.loading = undefined;
                break;

            default:
                self.cache = arg;
                self.loading = false;
                self.timer = setTimeout(onTimeout, Math.abs(self.ttl), self);
                break;
            }
        }
        else {
            self.loading = undefined;
        }
        for (var x in self.waiting) {
            process.nextTick(self.waiting[x], err, arg);
        }
        self.waiting = [];
        callback(err, arg);
    });

    var _this = null;
    if ('function' != typeof func) {
        _this = func;
        func = args.shift();
        assert('function' == typeof func);
    }
    func.apply(_this, args);
};

Leadof.prototype.expireNow = function () {
    process.nextTick(onTimeout, this);
};

Leadof.prototype.getLoadingState = function () {
    return this.loading;
};

Leadof.prototype.isLoaded = function () {
    return false == this.loading;
};

Leadof.prototype.wait = function (noMatterStartedOrNot, callback) {
    if ('function' == typeof noMatterStartedOrNot) {
        callback = noMatterStartedOrNot;
        noMatterStartedOrNot = false;
    }

    if (false === self.loading) {
        return process.nextTick(callback, null, self.cache);
    }

    if (noMatterStartedOrNot || true === self.loading) {
        self.waiting.push(callback);
        return;
    }
    // else should be undefined
    return process.nextTick(callback, 'Not started', 0);
};

module.exports = {
    create: function (name, ttl) {
        var lof = new Leadof(name, ttl);
        return lof;
    }
};
