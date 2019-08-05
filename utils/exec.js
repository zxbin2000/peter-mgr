let conExec;
let downloaded = 0;
let fileloaded = 0;
let download_failed = 0;
let fileload_failed = 0;
let exec = require('child_process').exec;
let fs = require('fs');
let utils = require('./utils');

function dowhile(func, cont, callback) {
    let retry = 0;
    function _doOnce(cb) {
        func(function (err, arg) {
            //console.log('func', err);
            if (null == err) {
                let x = cont(arg);
                if (true == x) {
                    return process.nextTick(function () {
                        _doOnce(cb);
                    });
                }
                if (false == x) {
                    return cb(null, arg);
                }
                // otherwise, should be undefined
                err = 'cont() tells to retry, but still fails';
            }
            if (retry++ < 3) {
                //console.log('retry', retry);
                return setTimeout(function () {
                    _doOnce(cb);
                }, 1000*3);
            }
            //console.log('callback', retry);
            cb(err, 0);
        });
    }
    _doOnce(callback);
}

function doExec(cmd, cont, callback) {
    function _do(callback) {
        console.log('RUN', cmd);
        exec(cmd, callback);
    }
    function _do2(callback) {
        conExec.run(_do, callback);
    }
    dowhile(undefined != conExec ? _do2 : _do, cont, callback);
}


/*
 *  options: {exists: String, clear: String, save: String,
 *            conctrl: concurrent, paging: String, list: bool, proj: [key, fields],
 *            validate: func, dont_read: true/false, mute: true/false}
 */
function downFile(nc, target, options, callback) {
    let exists, checkValid, isList;
    let paging, proj, path, conctrl, clear;
    let timeout;
    let dont_read = false;
    let mute = false;
    let pageno = 1;
    let results = [];

    if ('function' == typeof options) {
        callback = options;
        isList = false;
    }
    else {
        exists = options.exists;
        checkValid = options.validate;
        paging = options.paging;
        isList = options.list;
        proj = options.proj;
        if (undefined != proj)
            isList = true;
        path = options.save;
        conctrl = options.conctrl;
        clear = options.clear;
        timeout = options.timeout;
        if (options.dont_read) {
            dont_read = true;
        }
        if (options.mute) {
            mute = true;
        }
    }

    function _do(callback) {
        let url = paging ? (target+'&'+paging+'='+pageno) : target;
        if (!mute) {
            console.log('GET', nc.apiURL + url);
        }
        nc.get(url, {timeout: timeout}, function (err, arg) {
            callback(err, arg);
        });
        //if (timeout) {
        //    setTimeout(function () {
        //        if (!ret) {
        //            ret = true;
        //            callback('Timeout', null);
        //        }
        //    }, timeout);
        //}
        //nc.get(url, function (err, arg) {
        //    console.log(arg);
        //    callback(err, arg);
        //});

        //setTimeout(function () {
        //    //console.log('%s return', utils.now(), url);
        //    callback('err', null);
        //}, 500);
    }
    function _do2(callback) {
        conctrl.run(_do, callback);
    }
    function _cont(arg) {
        if (isList) {
            let n = 0;
            for (let i in arg.object) {
                n ++;
                results.push(arg.object[i]);
            }
            pageno ++;
            if (0!=n && undefined!=paging)
                return true;
            if (undefined != proj) {
                results = utils.projArray(results, proj[0], proj[1]);
            }
            arg = results;
        }
        // TODO:: paging but not list
        if (!checkValid || false!=checkValid(arg, false)) {
            return false;
        }
        return undefined;
    }

    function _dowhile() {
        dowhile(conctrl ? _do2 : _do, _cont, function (err, arg) {
            //console.log('_dowhileret', err);
            if (null == err) {
                downloaded ++;
                if (path) {
                    return utils.safeWrite(path, isList ? JSON.stringify(results) : arg, function (err) {
                        callback(err, isList ? results : arg);
                    });
                }
                return callback(null, isList ? results : arg);
            }
            download_failed ++;
            callback(err, arg);
        });
    }

    function _tryReadFromCache() {
        if (exists) {
            if (!dont_read) {
                return fs.readFile(exists, function (err, arg) {
                    if (null == err) {
                        try {
                            arg = arg.toString();
                            if (isList)
                                arg = JSON.parse(arg);
                        }
                        catch (e) {
                            return _dowhile();
                        }
                        assert(undefined == checkValid || checkValid(arg, true), 'Failed to check' + target);
                        return callback(null, arg);
                    }
                    _dowhile();
                });
            }
            return fs.open(exists, 'r', function (err, arg) {
                if (null == err) {
                    return fs.close(arg, function() {
                        callback(null, null);
                    });
                }
                _dowhile();
            });
        }
        _dowhile();
    }

    if (clear) {
        return fs.unlink(clear, function (err, arg) {
            _tryReadFromCache();
        });
    }
    _tryReadFromCache();
}

function loadFile(path, check, callback) {
    if (arguments.length == 2) {
        callback = check;
        check = undefined;
    }
    fs.readFile(path, function (err, arg) {
        if (null == err) {
            if (path.substring(path.length-5) == '.json') {
                arg = JSON.parse(arg.toString());
            }
            if (undefined != check && !check(arg)) {
                fileload_failed++;
                return callback('Corrupted file', 0);
            }
            fileloaded ++;
        }
        else {
            fileload_failed++;
        }
        callback(err, arg);
    });
}

module.exports = {
    dowhile: dowhile,
    doExec: doExec,
    downFile: downFile,
    loadFile: loadFile,
    stats: function () {
        return {
            downloaded: downloaded,
            fileloaded: fileloaded,
            download_failed: download_failed,
            fileload_failed: fileload_failed
        };
    }
};
