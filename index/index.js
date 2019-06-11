#!/usr/bin/env node

var engine = require('../engine/engine');
var assert = require('assert');
var utils = require('../utils/utils');

var str = engine.generate(
    utils.replaceStringTail(__filename, 'js', 'pp')
);

assert(null != str, "Failed to generate from index.pp");
//console.log(str);
eval(str);

function command(arg, rl) {
    function _help() {
        console.log("Commands for index:");
        console.log("help\tprint this message");
        console.log("create\tcreate an index group");
        console.log("    \targs: name [desc]");
        console.log("save\tsave an index");
        console.log("    \targs: group key value");
        console.log("load\tload an index");
        console.log("    \targs: group key");
        return null;
    }

    if (undefined == arg || arg.length < 1) {
        return _help();
    }

    var cmd = arg.shift();
    switch (cmd) {
        case 'help':
            return _help();

        case 'create':
            if (arg.length < 1) {
                return _help();
            }
            create(arg[0], arg[1], function (err, arg) {
                console.log(null == err ? "Succeed" : err);
            });
            break;

        case 'save':
            if (arg.length != 3) {
                return _help();
            }
            save(arg[0], arg[1], arg[2], function (err, arg) {
                console.log(null == err ? "Succeed" : err);
            });
            break;

        case 'load':
            load(arg[0], arg[1], function (err, arg) {
                console.log(null == err ? JSON.stringify(arg) : err);
            });
            break;

        default:
            load(cmd, arg[0], function (err, arg) {
                console.log(null == err ? JSON.stringify(arg) : err);
            });
            break;
    }

    return null;
}

module.exports = {
    init: init,
    create: create,
    save: save,
    load: load,
    command: command
};
