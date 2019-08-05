let assert = require('assert');
let sprintf = require("sprintf-js").sprintf;
let utils = require('../utils/utils');

function Lock(name) {
    this.name = name;
    this.waiting = [];
    this.owner = undefined;
}

Lock.prototype.lock = function (who, callback) {
    if ('function' == typeof who) {
        callback = who;
        who = 'noname';
    }
    assert(callback);

    if (this.owner) {
        this.waiting.push([who, callback]);
        return;
    }

    this.owner = who;
    process.nextTick(function () {
        callback(null, 0);
    });
};

Lock.prototype.unlock = function (who, callback) {
    if ('function' == typeof who) {
        callback = who;
        who = 'noname';
    }
    if (!who)
        who = 'noname';

    if (this.owner!=who && callback) {
        return process.nextTick(function () {
            callback('Not owner', 1);
        });
    }
    process.nextTick(function (self) {
        let next = self.waiting.shift();
        if (next) {
            self.owner = next[0];
            next[1](null, 1);
        }
        else {
            self.owner = undefined;
        }
        if (callback) {
            callback(null, 0);
        }
    }, this);
};

Lock.prototype.lockedBy = function () {
    return this.owner;
};

module.exports = {
    create: function (name) {
        let lock = new Lock(name);
        return lock;
    }
};