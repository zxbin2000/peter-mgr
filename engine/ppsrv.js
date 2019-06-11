var Engine = require('../engine/engine');
var Concurrent = require('../engine/concurrent');
var Parallel = require('../engine/parallel');
var Leadof = require('../engine/leadof');
var Lock = require('../engine/lock');
var Cache = require('../engine/cache');
var Sync = require('../engine/sync');
var DataStructure = require('../data_structure/_index');
var Postman = require('../utils/postman');
var Parser = require('../manager/parser');
var Exec = require('../utils/exec');
var moment = require('moment');
var assert = require('assert');
var express = require('express');
var multipart = require('connect-multiparty');
var bodyParser = require('body-parser');
var cookieParser = require('cookie-parser');
var utils = require('../utils/utils');
var Path = require('path');
var fs = require('fs');
var sprintf = require('sprintf-js').sprintf;

var peter;
var app;
var interfaces;
var _main, _onExit;
var serial = 0;
var cookie_parser;
var verbose = {url: 1, cost: 1, inf: 1};
var filter = new Map();
function genInterfaces(parsed) {
    var out = {};
    for (var i in parsed.rules) {
        var rule = parsed.rules[i];
        var proto = rule.proto;
        if (!rule.export)
            continue;

        var intf = [proto[0].name];
        for (var j=1; j<proto.length; j++) {
            intf.push(proto[j]);
        }
        out[proto[0].display] = intf;
    }

    return out;
}

function gen(file) {
    var _str = Engine.generate(file);
    assert(null != _str, 'Failed to generate from ' + file);
    try {
        eval(_str);
    }
    catch (e) {
        var file = process.cwd()+'/pp-gen-failed.js';
        fs.writeFileSync(file, _str);
        console.log('JS generated at', file);
        throw e;
    }

    var parsed = Engine.getLastParsed();
    interfaces = genInterfaces(parsed);
    for (var x in interfaces) {
        interfaces[x][0] = eval(interfaces[x][0]);
    }

    if (parsed.funcs.hasOwnProperty('main')) {
        _main = parsed.funcs['main'];
        _main.func = main;
    }
    if (parsed.funcs.hasOwnProperty('onExit')) {
        _onExit = onExit;
    }
    if (!parsed.options)
        return {};
    if (parsed.options.verbose) {
        if (parsed.options.verbose instanceof Array) {
            for (var x in parsed.options.verbose) {
                verbose[parsed.options.verbose[x]] = 1;
            }
        }
        else {
            for (var x in parsed.options.verbose) {
                verbose[x] = parsed.options.verbose[x];
            }
        }
    }
    if (parsed.options['cookie-parser']) {
        try {
            cookie_parser = eval(parsed.options['cookie-parser']);
        }
        catch (e) {
            console.log(e);
            throw e;
        }
    }
    if (parsed.options.filter) {
        var filename = parsed.options['filter'];
        var filePath = Path.resolve(Path.dirname(parsed.path), filename);
        var code = fs.readFileSync(filePath).toString();
        var filterArray = JSON.parse(code);
        for (var api_name in interfaces) {
            for (var i =0; i < filterArray.length; i++) {
                var item = filterArray[i];
                var expression = '^' + item.api + '$' ;
                var result = api_name.search(expression);
                if (result >= 0) {
                    var before = item.before;
                    if (before) {
                        item.before = eval(before);
                    }
                    var async = item.async;
                    if (async) {
                        item.async = eval(async);
                    }
                    filter.set(api_name, item);
                    break;
                }
            }
        }

    }
    return parsed.options;
}

function prepareParams(pnames, data, req, res, target) {
    var params = [];
    var pname, value, from;
    for (var i=1; i<pnames.length; i++) {
        pname = pnames[i];
        switch (pname.name) {
        case '__x':
            value = target;
            break;
        case '__arg':
            value = data;
            break;
        case '__http_headers':
            value = req.headers;
            break;
        case '__http_cookies':
            value = req.cookies;
            break;
        case '__http_files':
            value = req.files;
            break;
        case '__http_request':
            value = req;
            break;
        case '__http_response':
            value = res;
            break;
        default:
            from = pname.headers ? req.headers
                                 : (pname.cookies ? (cookie_parser ? cookie_parser(req.cookies) : req.cookies)
                                                  : (pname.files ? req.files
                                                                 : (pname.request ? req : data)));
            if (undefined===from || !from[pname.property]) out: {
                for (var x in from) {
                    if (x.toLowerCase() == pname.property.toLowerCase()) {
                        value = from[x];
                        break out;
                    }
                }
                if (!pname.optional) {
                    return pname.property;
                }
                value = undefined;
            }
            else {
                value = from[pname.property];
            }
            if (pname.files && value && !(value instanceof Array)) {
                value = [value];
            }
            break;
        }
        params.push(pname.default && value==='' ? true : value);
    }
    return params;
}

function printInterfaces(interfaces) {
    var arr = [];
    for (var x in interfaces) {
        arr.push([x, interfaces[x]]);
    }
    arr = arr.sort(function (a, b) {
        if (a[0] < b[0]) {
            return -1;
        }
        if (a[0] > b[0]) {
            return 1;
        }
        return 0;
    });

    var str = '{\n';
    for (var i in arr) {
        var inf = arr[i][1];
        str += '  ' + arr[i][0] + ': [';
        for (var j=1; j<inf.length; j++) {
            str += ((1==j) ? '' : ', ') + inf[j].display;
        }
        str += '], ';
        var filterItem =filter.get(arr[i][0]);
        if (filterItem) {
            var before = filterItem.before;
            if (before) {
                str = str + 'before: ' + before.name + ', ';
            }
            var async = filterItem.async;
            if (async) {
                str = str + 'async: ' + async.name + ', ';
            }
        }
        str += '\n';
    }
    str += '}';
    return str;
}

function _on(target, pnames, req, res, data) {
    var id = serial ++;
    var returned = false;
    if (verbose.url) {
        console.log('%s %s %s, #%d', utils.now(), req.method, req.url, id);
    }
    if (verbose.arg) {
        console.log('<-', utils.stringify(data));
    }
    if (verbose.cost) {
        var time = process.hrtime();
    }

    function _verbose(err, msg) {
        if (verbose.out) {
            console.log(err ? '->' : '=>', msg);
        }
        if (verbose.cost) {
            console.log('%s %s, #%d: %s', err ? 'ERR' : 'OK', req.url, id, utils.elapsed(time));
        }
    }

    function _error(code, msg, arg) {
        var ret = {error: msg};
        if (undefined != arg) {
            ret.data = arg;
        }
        _verbose(msg, utils.stringify(ret));
        try {
            res.status(code).json(ret);
        }
        catch (e) {
            console.log(e.stack);
        }
    }

    function _special(err, arg, res) {
        _verbose(null, utils.stringify(arg).substr(0, 4096));
        function commonCallback(err, arg) {
            if (null != err) {
                console.log(err, arg);
                _error(500, 'Internal Error');
            }
        }
        function tranHtml(data, res, type) {
            if (data instanceof Buffer || 'string' == typeof data) {
                if ('string' == typeof data) {
                    data = new Buffer(data);
                }
                res.setHeader('Content-Type', type ? type : 'text/html; charset=UTF-8');
                res.setHeader('Content-Length', data.length);
                res.end(data, commonCallback);
                return true;
            }
            return false;
        }

        var file;
        switch (err) {
        case '##DOWNLOAD##':
            file = arg;
            return res.download(file, Path.basename(file), commonCallback);

        case '##REDIRECT##':
            return res.redirect(302, arg);

        case '##HTML##':
            return tranHtml(arg, res);

        case '##ANY##':
            if (tranHtml(arg, res)) {
                return;
            }
            assert(arg instanceof Array);
            assert(arg.length == 2);
            var headers = arg[0];
            var data = arg[1];
            if (tranHtml(data, res, headers['content-type']))
                return;
            return res.json(data);

        case '##IMG##':
            res.setHeader('Content-Type', 'image/png');
            res.setHeader('Content-Length', arg.length);
            return res.end(arg, 'binary', commonCallback);

        case '##SENDFILE##':
            file = arg;
            return res.sendFile(file, {}, commonCallback);

        case '##BINARY##':  // arg or [options, arg]
            if (arg instanceof Buffer) {
                return res.end(arg, commonCallback);
            }
            assert(arg instanceof Array);
            assert(arg.length == 2);
            var options = arg[0];
            file = arg[1];

            if (options.hasOwnProperty('Content-Type')) {
                res.set({'Content-Type': options['Content-Type']});
            }
            if (options.hasOwnProperty('File-Name')) {
                var filename = options['File-Name'];
                var userAgent = (req.headers['user-agent'] || '').toLowerCase();
                if (userAgent.indexOf('msie') >= 0 || userAgent.indexOf('chrome') >= 0) {
                    res.setHeader('Content-Disposition', 'attachment; filename=' + encodeURIComponent(filename));
                }
                else if (userAgent.indexOf('firefox') >= 0) {
                    res.setHeader('Content-Disposition', 'attachment; filename*="utf8\'\'' + encodeURIComponent(filename) + '"');
                }
                else {
                    /* safari等其他非主流浏览器只能自求多福了 */
                    res.setHeader('Content-Disposition', 'attachment; filename=' + new Buffer(filename).toString('binary'));
                }
            }
            return res.end(file, 'binary', commonCallback);
        }
        _error(400, err, arg);
    }

    try {
        var params = prepareParams(pnames, data, req, res, target);
        if ('string' == typeof params) {
            return _error(400, 'Missing arguments', params);
        }
        params.push(function (err, arg) {
            if (returned) {
                throw new Error('Fatal: double callback from '+target);
            }
            returned = true;
            if (!err) {
                _verbose(null, utils.stringify(arg).substr(0, 4096));
                return res.json(arg);
            }
            if ('string' == typeof err) {
                if (err.substring(0, 2) == '##') {
                    return _special(err, arg, res);
                }
                return _error(400, err, arg);
            }
            _error(400, utils.stringify(err), arg);
        });
        var func = pnames[0];
        var filter_item = filter.get(target);
        if (filter_item) {
            var param = {
                "target": target,
                "request": req,
                "response": res,
                "data": data
            }
            var async = filter_item.async;
            if (async) {
                process.nextTick(function(){
                    try {
                        async(param, function (error, result) {
                            if (error) {
                                console.log(error);
                            }
                        });
                    } catch (e) {
                        console.log(e);
                    }
                });
            }
            var before = filter_item.before;
            if (before) {
                before(param,function () {
                    func.apply(null, params);
                })
            } else {
                func.apply(null, params);
            }

        } else {
            func.apply(null, params);
        }
    }
    catch (e) {
        console.log(e.stack);
        _error(500, 'Internal error', 'Error in calling ' + target + ': ' + e);
    }
}

function bindInterfaces(path, interfaces) {
    path = utils.appendTailIfNot(path, '/');

    if (verbose.inf) {
        console.log('Bind the following services to', path + '*');
        console.log(printInterfaces(interfaces));
    }

    app.get('/__list', function (req, res) {
        var out = {};
        for (var x in interfaces) {
            var arr = [];
            var inf = interfaces[x];
            for (var j=1; j<inf.length; j++) {
                arr.push(inf[j].display);
            }
            out[x] = arr;
        }
        res.json(out);
    });
    // setup handler
    for (var each in interfaces) {
        if (each == '_') {
            continue;
        }
        (function (name, inf) {
            var flag = false;
            for (var j = 1; j < inf.length; j++) {
                if (inf[j].files) {
                    app.post(path + name, multipart(), function (req, res) {
                        _on(name, inf, req, res, req.body);
                    });
                    flag = true;
                    break;
                }
            }
            if (!flag) {
                app.post(path + name, function (req, res) {
                    _on(name, inf, req, res, req.body);
                });
                app.get(path + name, function (req, res) {
                    _on(name, inf, req, res, req.query);
                });
            }
        })(each, interfaces[each]);
    }
    if (interfaces.hasOwnProperty('_')) {
        app.get(path + '*', function (req, res) {
            _on(req.params[0], interfaces['_'], req, res, req.query);
        });
        app.post(path + '*', function (req, res) {
            _on(req.params[0], interfaces['_'], req, res, req.body);
        });
    }
}

function call(funcname, params, callback) {
    var pnames = interfaces[funcname];
    if (undefined == pnames) {
        console.log('Invalid call', funcname);
        return;
    }
    params.push(callback);
    try {
        var func = pnames[0];
        func.apply(null, params);
    }
    catch (e) {
        console.log(e.stack);
        callback('Internal error', 'Error in calling ' + funcname + ': ' + e);
    }
}

function setup(pm, what, basedir, limit, nodefault) {
    assert(!app);

    peter = pm;
    app = express();
    limit = limit || '1024kb';
    app.use(bodyParser.json({type: 'application/json', limit: limit}));
    app.use(bodyParser.urlencoded({extended: true}));
    app.use(cookieParser());
    //设置跨域
    app.all('*', function(req, res, next) {
      res.header("Access-Control-Allow-Origin", req.headers.origin);
      res.header('Access-Control-Allow-Credentials', true);
      res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
      res.header("Access-Control-Allow-Methods", "PUT,POST,GET,DELETE,OPTIONS");
      res.header("X-Powered-By", '3.2.1');
      next();
    });

    if (!nodefault) {
        app.get('/', function (req, res, next) {
            var file = basedir + '/public/' + what + '.html';
            res.sendFile(file, {}, function (err, arg) {
                if (null == err) {
                    console.log(utils.now(), req.method + " " + req.url);
                    return;
                }
                next();
            });
        });

        app.get('/favicon.ico', function (req, res) {
            console.log(utils.now(), req.method + " " + req.url);

            var file = basedir + '/public/favicon.ico';
            res.sendFile(file, {}, function (err, arg) {
                if (null != err)
                    console.log('cannot find file: ', file);
            });
        });

        app.get('/static/:fileName', function (req, res) {
            console.log(utils.now(), req.method + " " + req.url);

            var fileName = req.params.fileName;
            res.sendFile(basedir + '/public/' + fileName, {}, function (err, arg) {
                if (null != err)
                    console.log('cannot find file:' + fileName);
            });
        });

        app.get('/bower_components/*', function (req, res) {
            console.log(utils.now(), req.method + " " + req.url);

            var filePath = req.path;
            res.sendFile(basedir + filePath, {}, function (err, arg) {
                if (null != err)
                    console.log('cannot find file:' + filePath);
            });
        });
    }

    return app;
}

module.exports = {
    pp: gen,
    genInterfaces: genInterfaces,
    bindInterfaces: bindInterfaces,
    bind: function (path) {
        if (path instanceof Array) {
            for (var x in path) {
                bindInterfaces(path[x], interfaces);
            }
        }
        else {
            bindInterfaces(path, interfaces);
        }
    },
    call: call,

    callMain: function (argv, conf, callback) {
        assert('function' == typeof callback, 'No callback specified');
        if (undefined == _main) {
            return process.nextTick(callback, null, 0);
        }
        try {
            var func = _main.func;
            switch (_main.proto.length) {
            case 1:
                func.call(null, callback);
                break;
            case 2:
                func.call(null, argv, callback);
                break;
            case 3:
                func.call(null, argv, conf, callback);
                break;
            }
        }
        catch (e) {
            console.log('Error in calling main:', e);
        }
    },

    callExit: function (callback) {
        if (undefined == _onExit) {
            return process.nextTick(callback, null, 0);
        }
        assert('function' == typeof callback);
        try {
            _onExit(callback);
        }
        catch (e) {
            console.log('Error in calling dump:', e);
        }
    },

    setup: setup
};
