let assert = require('assert');
let sprintf = require("sprintf-js").sprintf;
let utils = require('../utils/utils');

let all = [];

function Con(num, name, fNothingToDo) {
    this.key = all.length;
    this.allow = num;
    this.doing = 0;
    this.done = 0;
    this.name = name;
    this.q = [];
    this.fNothingToDo = fNothingToDo;
    this.cont = false;
    this.time = [0, 0];
    this.longtask = 0;
}

function _runOnce(con, cmd) {
    let _this = cmd.shift();
    let f = cmd.shift();
    let callback = cmd.pop();
    let time;

    //console.log('_runOnce1', con.doing, cmd.length);
    function _onRet() {
        process.nextTick(function () {
            con.done ++;
            con.doing --;
            _run(con);
            if (0==con.doing && 0==con.q.length && undefined!=con.fNothingToDo) {
                if (!con.fNothingToDo()) {
                    con.cont = false;
                }
            }
        });
    }

    cmd.push(function onret(err, arg) {
        let elapsed = process.hrtime(time);
        if (elapsed[0]) {       // > 1s
            con.longtask ++;
        }
        utils.accuTime(con.time, elapsed);

        callback(err, arg);
        _onRet();
    });

    con.doing ++;
    time = process.hrtime();
    try {
        f.apply(_this, cmd);
    }
    catch (e) {
        console.log(e.stack);
        process.nextTick(callback, e, 0);
        _onRet();
    }
}

function _run(con) {
    let cmd;
    while (con.doing < con.allow) {
        cmd = con.q.shift();
        if (!cmd) {
            break;
        }
        // console.log('_run', con.name, con.doing);
        _runOnce(con, cmd);
    }
}

Con.prototype.runArgs = function (args) {
    this.cont = true;

    if (this.doing<this.allow && 0==this.q.length) {
        return _runOnce(this, args);
    }

    this.q.push(args);
    _run(this);
};

Con.prototype.run = function (func) {
    assert(arguments.length > 1);
    assert('function' == typeof arguments[arguments.length-1]);

    let args = Array.prototype.slice.call(arguments);
    if ('function' == typeof func) {
        args.unshift(null);
    }
    else {
        assert('function' == typeof args[1]);
    }

    this.runArgs(args);
};

Con.prototype.isBusy = function () {
    return this.cont && 0 != (this.doing + this.q.length);
};

let runPrint = false;
let bVerbose = false;
function create(num, name, fNothingToDo) {
    let con = new Con(num, name, fNothingToDo);
    all.push(con);

    if (!runPrint) {
        runPrint = true;
        setInterval(printStatus, 1000*60);
    }
    return con;
}

function printTimeRelated(con) {
    let str = ' ';
    if (con.done) {
        str += utils.printTime(con.time, con.done) + ' ';
    }
    if (con.longtask) {
        str += '>1s:' + con.longtask;
    }
    return str;
}

function printStatus() {
    if (!bVerbose)
        return;
    let first = true;
    for (let i in all) {
        let con = all[i];
        if (con.cont && con.name) {
            if (first) {
                console.log('===========================  %s  ===========================', utils.now());
                first = false;
            }
            console.log(sprintf("#%-20s | Doing:%6d, Todo:%6d, Done:%8d |",
                con.name, con.doing, con.q.length, con.done), printTimeRelated(con));
        }
    }
}

module.exports = {
    create: create,

    isBusy: function (con) {
        if (undefined != con) {
            return con.isBusy();
        }
        for (let i in all){
            let con = all[i];
            if (con.cont && 0 != (con.doing + con.q.length))
                return true;
        }
        return false;
    },

    setVerbose: function (verbose) {
        bVerbose = verbose;
    },

    cleanup: function () {
        printStatus();
    }
};

