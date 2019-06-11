#!/usr/bin/env node

var peter, index, schema, utils;
try {
    peter = require('../manager/peter').createManager();
    index = require('../index/index');
    utils = require('../utils/utils');
} catch (e) {
    var _peter = require('peter');
    peter = _peter.createManager();
    index = _peter.Index;
    utils = _peter.Utils;
}
schema = peter.sm;

var Thenjs = require('thenjs');
var readline = require('readline');
var mongoaddr = utils.mongoAddrFromEnv(__dirname);

function run(arg) {
    var rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
    });

    function _help() {
        console.log("Commands:");
        console.log("help\tprint this message");
        console.log("schema\toperations for schema, run 'schema help' for details");
        console.log("peter\toperations for peter, run 'peter help' for details");
        console.log("index\toperations for index, run 'index help' for details");
        return null;
    }

    console.log("Please input command... ('help' for help, 'exit' to quit)");
    rl.prompt();

    var hijack = null;

    function onLine(cmd, arg) {
        if (null != hijack) {
            if (line != '$') {
                hijack(line);
            }
            else {
                hijack(null);
                hijack = null;
                rl.prompt();
            }
            return;
        }
        switch (cmd) {
            case 'help':
                _help();
                break;

            case 'schema':
                hijack = schema.command(arg, rl);
                break;

            case 'peter':
                hijack = peter.command(arg, rl);
                break;

            case 'index':
                index.command(arg, rl);
                break;

            case 'exit':
            case 'quit':
                rl.close();
                break;

            default:
                if (cmd != '') {
                    console.log('Unknown command: %s', cmd);
                }
                break;
        }
        if (null == hijack) {
            rl.prompt();
        }
    }

    if (undefined != arg) {
        var cmd = arg.shift();
        onLine(cmd, arg);
        return;
    }
    rl.on('line', function (line) {
        var arg = line.trim().split(' ');
        var cmd = arg.shift();
        onLine(cmd, arg);
    })
        .on('close', function () {
            console.log('Have a nice day!');
            process.exit(0);
        });
}

Thenjs(function (cont) {
    console.log("Fetching schemas...");
    peter.bindDb(mongoaddr, cont);
})
.then(function(cont, arg) {
    console.log("Okay, %d schema\nLoading index...", arg);
    index.init(peter, cont);
})
.then(function (cont, arg) {
    console.log("Okay");
    if (process.argv.length > 2) {
        var args = Array.from(process.argv);
        args.shift();
        args.shift();
        args.push(function (err, arg) {
            process.exit(0);
        });
        return run(args);
    }
    run();
})
.fail(function (cont, err, arg) {
    console.log(err.stack);
    console.error("Error: " + err);
    process.exit(-1);
});
