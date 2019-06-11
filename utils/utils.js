/**
 * Created by linshiding on 3/10/15.
 */
'use strict';

var moment = require('moment');
var assert = require('assert');
var sprintf = require('sprintf-js').sprintf;
var fs = require('fs');
var os = require('os');
var strip = require('strip-comment');
var jsonic = require('jsonic');
var schedule = require('node-schedule');
var config = require('config');

function timeString(time, format) {
    if (undefined == format) {
        format = 'YYYY-MM-DD HH:mm:ss';
    }
    return time ? moment(time instanceof Date ? time : new Date(time)).format(format) : time;
}

function now() {
    return timeString(new Date());
}

function parseJSON(str, use_jsonic) {
    if (use_jsonic) {
        var c = str.substr(0, 1);
        switch (c) {
        case '{':
        case '[':
            return jsonic(str);
        case '\'':
        case '"':
            if (str.substr(str.length - 1) == c)
                return str.substr(1, str.length - 2);
        }
        return str;
    }
    return JSON.parse(str);
}

function stringify(obj) {
    return jsonic.stringify(obj);
}

function isNull(obj) {
    return obj == null || obj !== obj;
}

function hasOwn(obj, key) {
    return Object.prototype.hasOwnProperty.call(obj, key);
}

function isEmpty(obj) {
    if (obj) {
        for (var key in obj) {
            return !hasOwn(obj, key);
        }
    }
    return true;
}

function clear(obj) {
    for (var x in obj) {
        delete obj[x];
    }
    return obj;
}

function cmpObj(obj1, obj2, fields) {
    if (undefined == fields) {
        var type1 = typeof obj1;
        var type2 = typeof obj2;

        if (type1 != type2) {
            return sprintf('-type [%s] [%s]', type1, type2);
        }
        if (type1 != 'object') {
            if (obj1 !== obj2) {
                return sprintf('-value [%s] [%s]', obj1, obj2);
            }
            return null;
        }

        var keys1 = Object.keys(obj1);
        var keys2 = Object.keys(obj2);
        if (keys1.length != keys2.length) {
            return sprintf('-keys [%s] [%s]', keys1, keys2);
        }
        for (var x in obj1) {
            var str = cmpObj(obj1[x], obj2[x]);
            if (str) {
                return x + ':' + str;
            }
        }
        return null;
    }
    if (fields instanceof Array) {
        for (var i = 0; i < fields.length; i++) {
            var str = cmpObj(obj1[fields[i]], obj2[fields[i]]);
            if (str) {
                return '#' + i + ':' + str;
            }
        }
        return null;
    }
    var str = cmpObj(obj1[fields], obj2[fields]);
    if (str) {
        return fields + ':' + str;
    }
    return null;
}

function cleanAssociateArray(arr) {
    var out = [];
    for (var x in arr) {
        out.push(arr[x]);
    }
    return out;
}

function objLen(obj) {
    return Object.keys(obj).length;
}

function resJSON(code, msg, data) {
    var result = {};
    result.code = code;
    result.msg = msg;
    result.timestamp = now();
    result.data = data;
    return result;
}

function partial(obj, fields) {
    var out = {};
    if (fields instanceof Array) {
        for (var i = 0; i < fields.length; i++) {
            var x = fields[i];
            if (obj.hasOwnProperty(x)) {
                out[x] = obj[x];
            }
        }
        return out;
    }
    if (obj.hasOwnProperty(fields)) {
        out[fields] = obj[fields];
        return out;
    }
    return (!fields) ? obj : out;
}

function arr2json(arr, fields) {
    var out = {}
        , x;

    assert(arr instanceof Array);
    assert(fields instanceof Array);
    assert(arr.length >= fields.length);

    for (var i = 0; i < fields.length; i++) {
        out[fields[i]] = arr[i];
    }
    return out;
}

function translateKeyArray(keyarr, map) {
    var out = [];
    for (var i = 0; i < keyarr.length; i++) {
        out.push(map[keyarr[i]]);
    }
    return out;
}

function pickColumnFromArray(arrayOfArray, column) {
    var out = [];
    for (var i = 0; i < arrayOfArray.length; i++) {
        out.push(arrayOfArray[i][column]);
    }
    return out;
}

function projArray(arr, key, fields) {
    var out = {};
    if (fields instanceof Array) {
        for (var x in arr) {
            var res = [];
            for (var y in fields) {
                res.push(arr[x][fields[y]]);
            }
            out[arr[x][key]] = res;
        }
        return out;
    }

    if (fields) {
        for (var x in arr) {
            out[arr[x][key]] = arr[x][fields];
        }
        return out;
    }

    for (var x in arr) {
        out[arr[x][key]] = arr[x];
    }
    return out;
}

function projObj(arr, key, fields) {
    var out = {};
    for (var x in arr) {
        out[arr[x][key]] = partial(arr[x], fields);
    }
    return out;
}

function reduceArray(arr, keys) {
    if ('string' == typeof keys) {
        keys = [keys];
    }
    else {
        assert(keys instanceof Array);
        if ('function' == typeof keys[0]) {
            keys = [keys];
        }
    }

    //console.log(keys);
    function proc(inputs, n) {
        var func, field;
        if ('string' == typeof keys[n]) {
            func = function (value) {
                return value;
            };
            field = keys[n];
        }
        else {
            func = keys[n][0];
            field = keys[n][1];
        }

        var out = {};
        for (var x in inputs) {
            var what = func(inputs[x][field]);
            if (!out[what]) {
                out[what] = [];
            }
            out[what].push(inputs[x]);
        }
        for (var x in out) {
            if (n != keys.length-1) {
                out[x] = proc(out[x], n+1);
            }
            else {
                out[x] = out[x].length;
            }
        }
        return out;
    }
    return proc(arr, 0);
}

function partialArray(arr, fields) {
    var out = [];
    for (var i in arr) {
        out.push(partial(arr[i], fields));
    }
    return out;
}

function copyArray(arr) {
    var newarr = [];
    for (var x in arr) {
        newarr.push(arr[x]);
    }
    return newarr;
}

function uniqueArray(arr) {
    var result = [], hash = {};
    var elem;
    for (var i=0; i<arr.length; i++) {
        elem = arr[i];
        if (null!=elem && !hash[elem]) {
            result.push(elem);
            hash[elem] = true;
        }
    }
    return result;
}

function mergeArray(arr1, arr2) {
    var arr = arr1;
    arr.concat(arr2);
    return uniqueArray(arr);
}

function shuffleArray(arr) {        // in-place random shuffling
    function getRandomInt(min, max) {       //[min, max)
        min = Math.ceil(min);
        max = Math.floor(max);
        return Math.floor(Math.random() * (max - min)) + min;
    }
    var len = arr.length;
    var x, t;
    // random shuffle
    for (var i=0; i<len; i++) {
        x = getRandomInt(0, len);
        if (i != x) {
            t = arr[x];
            arr[x] = arr[i];
            arr[i] = t;
        }
    }
    return arr;
}

function retcallback(arg) {
    assert(arg.length == 3);
    var callback = arg[0];
    process.nextTick(function () {
        callback(arg[1], arg[2]);
    });
}

function replaceStringTail(string, from, to) {
    return string.substring(0, string.length - from.length) + to;
}


// conf lookup order: cwd -> start script dir
function mongoAddrFromEnv(start_script_dir) {
    var dir = [process.cwd()];
    if (undefined!=start_script_dir && start_script_dir!=dir[0])
        dir.push(start_script_dir);

    var host = os.hostname();
    var env, mongoaddr;
    for (var x in dir) {
        env = loadJsonFile(dir[x]+'/env.json');
        if (null != env) {
            mongoaddr = env[host];
            if (undefined == mongoaddr)
                mongoaddr = env['dev'];
            console.log('Found env.json in', dir[x]);
            console.log('Using ' + mongoaddr);
            return mongoaddr;
        }
    }
    console.log('Warning: env.json is not found in [' + dir + ']');
    return null;
}

// conf lookup from config/env.json
function mongoAddrFromConf() {
    return config.get('schema');
}

// !! need to be called with getRealArgv(__filename)
function getRealArgv(script_path) {
    assert(undefined != script_path);
    var argv = Array.from(process.argv);
    while (argv[0]) {
        var arg_path = appendTailIfNot(argv[0], '.js');
        try {
            arg_path = fs.realpathSync(arg_path);
            if (arg_path == script_path) {
                break;
            }
        }
        catch (e) {}
        argv.shift();
    }
    return argv;
}

function accuTime(time, elapsed) {
    time[0] += elapsed[0];
    time[1] += elapsed[1];
    if (time[1] >= 1e9) {
        time[0] += 1;
        time[1] -= 1e9;
    }
}

function printTime(time, count) {
    count = count || 1;

    var nano = time[0] * 1e9 + time[1];
    var f = nano / count;
    var unit = [['ns', 1000], ['us', 1000], ['ms', 1000], ['s', 60], ['m', 60], ['h', 24], ['day', 30]];
    var n;

    for (n=0; n<unit.length-1; n++) {
        if (f >= unit[n][1]) {
            f /= unit[n][1];
        }
        else {
            break;
        }
    }
    return sprintf("%.2f%s", f, unit[n][0]);
}

function elapsed(start_time) {
    var time = process.hrtime(start_time);
    return printTime(time, 1);
}

function printSize(num) {
    var unit = ['B', 'KB', 'MB', 'GB'];
    var n;

    for (n=0; n<3; n++) {
        if (num >= 1024.0) {
            num /= 1024.0;
        }
        else {
            break;
        }
    }
    return n == 0
        ? sprintf("%fB", num)
        : sprintf("%.2f%s", num, unit[n]);
}

function safeWrite(path, data, callback) {
    if (!path || !data) {
        return process.nextTick(function () {
            callback(null, data);
        });
    }
    var tmp = path + '.tmp';
    if ('string' != typeof data && !(data instanceof Buffer)) {
        data = JSON.stringify(data);
    }
    fs.writeFile(tmp, data, function (err, arg) {
        if (null == err) {
            return fs.rename(tmp, path, callback);
        }
        callback(err, arg);
    });
}

function loadJsonFile(file, options) {
    var encoding, _default, jsonic;
    if (options) {
        encoding = options.encoding;
        _default = options.default;
        jsonic = options.jsonic;
    }
    try {
        var str = strip.js(fs.readFileSync(file).toString(encoding));
        return parseJSON(str, jsonic);
    }
    catch (e) {
        if ('ENOENT' != e.code) {
            console.log('Warning: failed to parse json file', file);
            console.log(e.stack);
        }
        return _default ? _default : null;
    }
}

function loadJsonFileAsync(file, options, callback) {
    if (undefined == callback) {
        assert('function' == typeof(options));
        callback = options;
        options = undefined;
    }

    var encoding, _default, jsonic;
    if (options) {
        encoding = options.encoding;
        _default = options.default;
        jsonic = options.jsonic;
    }
    try {
        fs.readFile(file, function (err, arg) {
            if (null == err) {
                var str = arg.toString(encoding);
                try {
                    str = strip.js(str);
                    arg = parseJSON(str, jsonic);
                }
                catch (e) {
                    console.log('Warning: failed to parse json file', file);
                    console.log(e.stack);
                }
            }
            else {
                arg = (undefined != _default)
                    ? _default
                    : null;
            }
            callback(null, arg);
        });
    }
    catch (e) {
        process.nextTick(function () {
            callback(null, (undefined != _default)
                         ? _default
                         : null);
        });
    }
}

function setif(src, dest) {
    assert(src);
    for (var x in dest) {
        if (src.hasOwnProperty(x)) {
            if ('object' == typeof dest[x]) {
                setif(src[x], dest[x]);
            }
            else {
                dest[x] = src[x];
            }
        }
    }
}

function loadConfFile(file, getconf) {
    var conf = loadJsonFile(file);
    if (null != conf) {
        setif(conf, getconf);
    }
    return conf;
}

function limitDisplayLength(str, len) {
    var n = 0;
    for (var x=0; x<str.length && n<len; x++) {
        var c = str.charCodeAt(x);
        n += (c<255) ? 1 : 2;
    }
    var out = str.substr(0, x);
    for ( ; n<len; n++)
        out += ' ';
    return out;
}

function blankStr(len) {
    var str = '';
    for (var i=0; i<len; i++) {
        str += ' ';
    }
    return str;
}

function sortArrWithTime(arr, time_col, descending) {
    if (undefined != arr && null != arr) {
        arr.sort(descending
            ? function (a, b) {
                return Date.parse(b[time_col]) - Date.parse(a[time_col]);
            }
            : function (a, b) {
                return Date.parse(a[time_col]) - Date.parse(b[time_col]);
            }
        );
    }
    return arr;
}

function appendTailIfNot(str, tail) {
    var len1 = str.length;
    var len2 = tail.length;
    if (str.substring(len1-len2) != tail) {
        str += tail;
    }
    return str;
}

function removeTailIf(str, tail) {
    var len1 = str.length;
    var len2 = tail.length;
    if (str.substring(len1-len2) == tail) {
        str = str.substring(0, len1-len2);
    }
    return str;
}

function copyObj(to, from) {
    for (var x in from) {
        to[x] = from[x];
    }
}

function repeat(func, interval) {
    func(function cb(err, arg) {
        setTimeout(function () {
            func(cb);
        }, translateIntervalString(interval));
    });
}

// callback will be called only at the first time
function runThenRepeat(func, interval, callback) {
    func(function (err, arg) {
        if (null == err) {
            (function doAgain() {
                setTimeout(func, translateIntervalString(interval), doAgain);
            })();
            return callback(null, arg);
        }
        callback(err, arg);
    });
}

// args: timeout, args..., func
function delay(timeout, args) {
    var args = Array.prototype.slice.call(arguments);
    args.shift();       // timeout already there
    var func = args.pop();
    assert('function' == typeof func);

    if (!args.length)
        args.push(null);

    args.push(translateIntervalString(timeout));
    args.push(func);
    setTimeout.apply(null, args);
}

function timer(func, cron) {
    schedule.scheduleJob(cron, function() {
        func(function() {
            console.log('task done', now());
        });
    });
}

function parseDate(str, next) {
    if (str.length != 8 && str.length != 6 && str.length != 4)
        return null;

    var year = +str.substr(0, 4);
    var month, day;
    switch (str.length) {
        case 4:
            month = 1;
            day = 1;
            if (next)
                year += 1;
            break;
        case 6:
            month = +str.substr(4, 2);
            day = 1;
            if (next)
                month += 1;
            break;
        case 8:
            month = +str.substr(4, 2);
            day = +str.substr(6, 2);
            if (next)
                day += 1;
            break;
    }
    return new Date(year, month-1, day);
}

function translateIntervalString(str) {
    switch (typeof str) {
    case 'number':
        return str;
    case 'string':
        break;
    case 'undefined':
        return undefined;
    default:
        if (null == str)
            return null;
        assert(false, 'Wrong type of interval:'+str);
        break;
    }
    var t = parseInt(str);
    if (isNaN(t)) {
        t = 1;
    }
    else {
        str = str.substr(t.toString().length);
    }
    switch (str.toLowerCase()) {
    case 'd':
    case 'day':
    case 'days':
        t *= 86400000;
        break;
    case 'h':
    case 'hour':
    case 'hours':
        t *= 3600000;
        break;
    case 'm':
    case 'min':
    case 'minute':
    case 'minutes':
        t *= 60000;
        break;
    case 's':
    case 'sec':
    case 'second':
    case 'seconds':
    default:
        t *= 1000;
        break;
    }
    return t;
}

function translateTimeSpan(str) {
    var tStart, tEnd;
    var now = new Date();
    switch (str) {
        case 'today':
            tStart = new Date(now.getFullYear(), now.getMonth(), now.getDate());
            tEnd = new Date(now.getFullYear(), now.getMonth(), now.getDate()+1);
            break;

        case 'yesterday':
            tStart = new Date(now.getFullYear(), now.getMonth(), now.getDate()-1);
            tEnd = new Date(now.getFullYear(), now.getMonth(), now.getDate());
            break;

        case 'this week':
            var tDay = now.getDay();
            tStart = new Date(now.getFullYear(), now.getMonth(), now.getDate() - (0==tDay ? 6 : tDay - 1));
            tEnd = new Date(now.getFullYear(), now.getMonth(), now.getDate() + 1);
            break;

        /*case 'last week':
         var tDay = now.getDay();
         tStart = new Date(now.getFullYear(), now.getMonth(), now.getDate() - (0==tDay ? 6 : tDay - 1));
         tEnd = new Date(now.getFullYear(), now.getMonth(), now.getDate() + 1);
         break;
         */
        case 'this month':
            tStart = new Date(now.getFullYear(), now.getMonth(), 1);
            tEnd = new Date(now.getFullYear(), now.getMonth()+1, 1);
            break;

        case 'last month':
            tStart = new Date(now.getFullYear(), now.getMonth()-1, 1);
            tEnd = new Date(now.getFullYear(), now.getMonth(), 1);
            break;

        default:
            var arr = str.search('~') != -1 ? str.split('~') : str.split('-');
            if (2 == arr.length) {
                if (arr[1].toLowerCase() == 'now') {
                    tEnd = new Date(Date.now());
                    tStart = parseDate(arr[0]);
                }
                else {
                    if (arr[0].length != arr[1].length) {
                        return null;
                    }
                    tStart = parseDate(arr[0]);
                    tEnd = parseDate(arr[1], true);
                }
            }
            else {
                tStart = parseDate(arr[0]);
                tEnd = parseDate(arr[0], true);
            }
            break;
    }
    //console.log(tStart);
    //console.log(tEnd);
    return (!tStart || !tEnd) ? null : [tStart, tEnd];
}

module.exports = {
    timeString: timeString,
    now: now,                      // 返回一个YYYY-MM-DD HH:mm:ss的标准时间字符串
    accuTime: accuTime,
    elapsed: elapsed,
    printTime: printTime,
    printSize: printSize,
    limitDisplayLength: limitDisplayLength,
    blankStr: blankStr,
    parseDate: parseDate,
    translateIntervalString: translateIntervalString,
    translateTimeSpan: translateTimeSpan,

    isNull: isNull,                // 判断是否为空，废弃，建议使用underscore
    isEmpty: isEmpty,              // 判断是否为空值，废弃，建议使用underscore
    cmpObj: cmpObj,
    clear: clear,
    objLen: objLen,
    cleanAssociateArray: cleanAssociateArray,
    sortArrWithTime: sortArrWithTime,

    resJSON: resJSON,              // 生成返回信息
    parseJSON: parseJSON,
    stringify: stringify,
    retcallback: retcallback,
    loadJsonFile: loadJsonFile,
    loadJsonFileAsync: loadJsonFileAsync,
    loadConfFile: loadConfFile,

    partial: partial,
    arr2json: arr2json,
    translateKeyArray: translateKeyArray,
    pickColumnFromArray: pickColumnFromArray,
    projArray: projArray,
    reduceArray: reduceArray,
    partialArray: partialArray,
    copyArray: copyArray,
    uniqueArray: uniqueArray,
    mergeArray: mergeArray,
    shuffleArray: shuffleArray,
    copyObj: copyObj,
    projObj: projObj,

    replaceStringTail: replaceStringTail,
    appendTailIfNot: appendTailIfNot,
    removeTailIf: removeTailIf,

    mongoAddrFromEnv: mongoAddrFromEnv,
    mongoAddrFromConf: mongoAddrFromConf,
    getRealArgv: getRealArgv,
    safeWrite: safeWrite,

    delay: delay,
    repeat: repeat,
    runThenRepeat: runThenRepeat,

    split: require('./split')
};
