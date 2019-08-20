#!/usr/bin/env node

let peter, index, schema, utils;
try {
    peter = require('../index').getManager();
    index = require('../index/index');
    utils = require('../utils/utils');
} catch (e) {
    let _peter = require('peter-mgr');
    peter = _peter.createManager();
    index = _peter.Index;
    utils = _peter.Utils;
}
schema = peter.sm;

let readline = require('readline');
let mongoaddr = utils.mongoAddrFromConf();

function run(arg) {
    let rl = readline.createInterface({
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

    let hijack = null;

    function onLine(cmd, arg) {
        if (null != hijack) {
            if (line != '$') {
                hijack(line);
            } else {
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
        let cmd = arg.shift();
        onLine(cmd, arg);
        return;
    }
    rl.on('line', function (line) {
        let arg = line.trim().split(' ');
        let cmd = arg.shift();
        onLine(cmd, arg);
    }).on('close', function () {
        console.log('Have a nice day!');
        process.exit(0);
    });
}

peter.bindDb(mongoaddr, { useNewUrlParser: true }, (error, args) => {
    if(error) {
        console.log('Error: ', error);
        process.exit(-1);
    }
    console.log("Okay, %d schema\nLoading index...", args);
    index.init(peter, (error, args) => {
        console.log("Okay");
        if (process.argv.length > 2) {
            let args = Array.from(process.argv);
            args.shift();
            args.shift();
            args.push(function (err, arg) {
                process.exit(0);
            });
            return run(args);
        }
        run();
    });
});
