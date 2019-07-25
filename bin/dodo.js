#!/usr/bin/env node

let peter, index, utils, engine, ppsrv;
let Concurrent, Parallel, Leadof, Lock, Cache, Sync;
let Postman, Parser, DataStructure, Exec;
try {
    peter = require('../manager/peter').createManager();
    index = require('../index/index');
    utils = require('../utils/utils');
    engine = require('../engine/engine');
    ppsrv = require('../engine/ppsrv');
    Concurrent = require('../engine/concurrent');
    Parallel = require('../engine/parallel');
    Leadof = require('../engine/leadof');
    Lock = require('../engine/lock');
    Cache = require('../engine/cache');
    Sync = require('../engine/sync');
    Postman = require('../utils/postman');
    Parser = require('../manager/parser');
    DataStructure = require('../data_structure/_index');
    Exec = require('../utils/exec');
} catch (e) {
    console.log('====', e);
    let _peter = require('peter-mgr');
    peter = _peter.createManager();
    index = _peter.Index;
    utils = _peter.Utils;
    engine = _peter.Engine;
    ppsrv = _peter.Service;
    Concurrent = _peter.Concurrent;
    Parallel = _peter.Parallel;
    Leadof = _peter.Leadof;
    Lock = _peter.Lock;
    Cache = _peter.Cache;
    Postman = _peter.Postman;
    Sync = _peter.Sync;
    Parser = _peter.Parser;
    DataStructure = _peter.DataStructure;
    Exec = _peter.Exec;
}
let moment = require('moment');
let fs = require('fs');
let assert = require('assert');
let sprintf = require("sprintf-js").sprintf;
let Path = require('path');

let mongoAddr;
let bRepeat = false;
let tRepeat = '10second';
let bTimer = false;
let cRepeat = '0 0 9 * * *';
let bService = false;
let argv, what, _main, _onExit = process.nextTick;
let app, conf, port, target, conf_file;

// to include helper js
//var __code__ = fs.readFileSync(__dirname+'/dodo-helper.js').toString();
//eval(__code__);
let dowhile = Exec.dowhile;
let downFile = Exec.downFile;
let doExec = Exec.doExec;
let loadFile = Exec.loadFile;

try {
    run();
} catch (e) {
    console.log(e.stack);
    process.exit(-1);
}

function run() {
    procArgv();
    commonSetup();

    what = argv[0];
    if (what.substring(what.length - 3) != '.pp')
        what += '.pp';
    argv[0] = what;
    let basedir = Path.resolve(process.cwd(), Path.dirname(what));
    function _procconf(action, conf_in_pp) {
        let conf = {};
        if (conf_in_pp) {
            utils.copyObj(conf, conf_in_pp);
        }

        if (!conf_file) {
            conf_file = basedir + '/' + Path.basename(what, '.pp') + '.json';
        }
        let conf_in_conf = utils.loadJsonFile(conf_file, {jsonic: true, default: null});
        if (conf_in_conf) {
            console.log(action, what, 'with conf', conf_file);
            utils.copyObj(conf, conf_in_conf);
        } else {
            console.log(action, what);
        }
        return conf;
    }

    let options;
    if (bService) {
        options = ppsrv.pp(what);
        if (!mongoAddr && options.mongo) {
            mongoAddr = options.mongo;
        }
        conf = _procconf('Serving', options ? options.conf : null);
        if (!mongoAddr) {
            mongoAddr = conf.mongo ? conf.mongo : utils.mongoAddrFromConf();
        }
        _onExit = ppsrv.callExit;
        app = ppsrv.setup(peter, what, basedir, '40mb', options.no_default_inf);
    } else {
        options = genPP(what);
        if (!mongoAddr && options.mongo) {
            mongoAddr = options.mongo;
        }
        conf = _procconf('Doing', options ? options.conf : null);
        if (!mongoAddr && conf.mongo) {
            mongoAddr = conf.mongo;
        }
    }

    Concurrent.setVerbose(true);
    if (mongoAddr) {
        console.log("Fetching schemas...");
        peter.bindDb(mongoAddr, function (err, arg) {
            if (err) {
                throw new Error('peter.bindDb: ' + (err.stack ? err.stack : err));
            }
            console.log("Okay, %d schema", arg);
            runPP(_exit);
        });
    } else {
        runPP(_exit);
    }

    function _exit(err, arg) {
        if (null != err) {
            console.error('Error: ' + (err.stack ? err.stack : JSON.stringify(err)));
        }
        prepareExit((null == err) ? 0 : -1);
    }
}

function genPP(what) {
    let str = engine.generate(what);
    assert(null != str, "Failed to generate from " + what);
    try {
        eval(str);
    } catch (e) {
        let file = process.cwd() + '/pp-gen-failed.js';
        fs.writeFileSync(file, str);
        console.log('JS generated at', file);
        throw e;
    }
    let parsed = engine.getLastParsed();
    if (parsed.funcs.hasOwnProperty('main')) {
        _main = parsed.funcs['main'];
        _main.func = main;
    } else {
        throw new Error('main is not defined');
    }
    if (parsed.funcs.hasOwnProperty('onExit')) {
        _onExit = onExit;
    }
    return parsed.options;
}

function runMain(callback) {
    console.log('Now:', utils.now());

    let time = process.hrtime();
    function cb(err, arg) {
        if (Exec.stats.downloaded || Exec.stats.download_failed
            || Exec.stats.fileloaded || Exec.stats.fileload_failed
        ) {
            console.log('============================================================================');
            console.log('Downloaded: %d, Failed: %d\nFile Loaded: %d, Failed: %d\nCost: %s',
                Exec.stats.downloaded, Exec.stats.download_failed, Exec.stats.fileloaded, Exec.stats.fileload_failed,
                utils.elapsed(time));
        }
        callback(err, arg);
    }

    let func = _main.func;
    switch (_main.proto.length) {
        case 1:
            func.call(null, cb);
            break;
        case 2:
            func.call(null, argv, cb);
            break;
        case 3:
            func.call(null, argv, conf, cb);
            break;
    }
}

function runPP(callback) {
    if (!bService) {
        if (bRepeat) {
            console.log('Run %s every %s...', what, tRepeat);
            return utils.repeat(runMain, tRepeat);
        } else if (bTimer) {
            console.log('Run %s when %s...', what, cRepeat);
            return utils.timer(runMain, cRepeat);
        }
        runMain(callback);
    } else {
        ppsrv.callMain(argv, conf, function (err, arg) {
            if (null == err) {
                if (!conf.port) {
                    console.log('Error: port is not specified');
                    return callback('Invalid conf', 0);
                }
                ppsrv.bind(conf.target || '/');
                let server = app.listen(conf.port, function (err, arg) {
                    assert(null == err, 'Failed to listen to ' + conf.port + ', change?');
                    console.log('Now:', utils.now());
                    console.log('Listening on %d...', server.address().port);
                });
            } else {
                callback(err, arg);
            }
        });
    }
}

var exiting = false;
function prepareExit(code) {
    if (!exiting) {
        try {
            exiting = true;
            return _onExit(function () {
                process.exit(code);
            });
        }
        catch (e) {}
    }
    process.exit(code);
}

function commonSetup() {
    process.on('exit', function(code) {
        Concurrent.cleanup();
        console.log(utils.now(), 'exit with code:', code);
    });
    process.on('uncaughtException', function(err) {
        console.log(utils.now(), 'Caught exception: ' + err.toString().substring(0, 512));
        console.log(err.stack);
        prepareExit(-1);
    });
    process.on('SIGINT', function() {
        console.log(utils.now(), 'Got SIGINT.');
        prepareExit(-1);
    });
    process.on('SIGTERM', function() {
        console.log(utils.now(), 'Got SIGTERM.');
        prepareExit(-1);
    });
    process.on('SIGHUP', function() {
        console.log(utils.now(), 'Got SIGHUP, ignored');
    });
    process.on('SIGUSR1', function() {
        console.log(utils.now(), 'Got SIGUSR1, ignored');
    });
    process.on('SIGUSR2', function() {
        console.log(utils.now(), 'Got SIGUSR2, ignored');
    });
}

function procArgv() {
    argv = utils.getRealArgv(__filename);
    if (argv.length < 2) {
        console.log('node dodo ppfile [args]');
        process.exit(-1);
    }
    argv.shift();

    for (let i = 0; i < argv.length; i++) {
        if (argv[0] == '-r') {
            bRepeat = true;
            tRepeat = argv[1];
            argv.shift();
            argv.shift();
        } else if (argv[0] == '-s') {
            bService = true;
            argv.shift();
        } else if (argv[0] == '-c') {
            conf_file = argv[1];
            argv.shift();
            argv.shift();
        } else if (argv[0] == '-t') {
            bTimer = true;
            cRepeat = argv[1];
            argv.shift();
            argv.shift();
        } else {
            break;
        }
    }

    let track_gc_object_stats = false;
    //var v8 = require('v8');
    //v8.setFlagsFromString('--trace_gc');
    //setTimeout(function() { v8.setFlagsFromString('--notrace_gc'); }, 60e3);
    if (track_gc_object_stats) {
        setInterval(function () {
            let heap = getV8Statistics(); //v8.getHeapStatistics();
            //console.log(heap);
            let usage = process.memoryUsage();
            console.log('rss: %s  heapTotal: %s  heapUsed: %s, external_allocated: %s', utils.printSize(usage['rss']),
                utils.printSize(usage['heapTotal']), utils.printSize(usage['heapUsed']), utils.printSize(heap['amount_of_external_allocated_memory']));

            for (let x in heap) {
                if (heap[x] > 100 * 1024 * 1024)
                    console.log(x, utils.printSize(heap[x]));
            }
        }, 60 * 1000);
    }
    return argv;
}
