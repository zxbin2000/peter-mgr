/**
 * Created by linshiding on 3/10/15.
 */

let Parser = require('./parser');
let MongoOP = require('./mongoop');
let Engine = require('../engine/engine');
let assert = require('assert');
let Thenjs = require('thenjs');
let utils = require('../utils/utils');
let ascii = require('../utils/ascii');
let os = require('os');
let fs = require('fs');
let zlib = require('zlib');
let sprintf = require('sprintf-js').sprintf;

function Manager() {
    this.SchemaByName = {};
    this.SchemaByKey = {};
    this.collection = null;
}

function timeString(time) {
    return utils.timeString(time);
}

function addSchema(sm, sch, check) {
    if (sch.__name__.substr(0, 1) == '@') {
        assert(sch.hasOwnProperty('__key__'));
    }
    if (check) {
        assert(!sm.SchemaByName.hasOwnProperty(sch.__name__));
        assert(!sm.SchemaByKey.hasOwnProperty(sch.__key__));
    }
    sm.SchemaByName[sch.__name__] = sch;
    sm.SchemaByKey[sch.__key__] = sch;
}

function genSchemaKey(sm, sch, callback) {
    let key;

    if (sm.SchemaByName.hasOwnProperty(sch.__name__)) {
        key = sm.SchemaByName[sch.__name__].__key__;
        assert(key > 0);
        return process.nextTick(function () {
            callback(null, key);
        });
    }

    Thenjs(function (cont) {
        // generate a unique key
        MongoOP.add(sm.collection, 0, 'lastkey', 1, cont);
    })
    .then(function (cont, arg) {
        key = arg.lastkey;
        let elem = {
            name: sch.__name__,
            key: key,
            time: sch.__time__,
            who: sch.__who__,
            id: 0
        };
        // try to set key of this schema
        MongoOP.pushMap(sm.collection, 0, 'schema', 'name', elem, true, cont);
    })
    .then(function (cont, arg) {
        callback(null, key);
    }, function (cont, err) {
        if (err == 'Already existed') {
            MongoOP.getElementsByCond(sm.collection, 0, 'schema', {name: sch.__name__}, cont);
        }
        else {
            callback(err, 0);
        }
    })
    .then(function (cont, arg) {
        key = arg[0].key;
        // TODO: update local schema from the DB, then we can check the compatibility
        callback(null, key);
    });
}

let qUpdating = [];
function updateSchema(sm, sch, callback) {
    assert(!sch.hasOwnProperty('_id') && !sch.hasOwnProperty('__key__'));

    sch['__time__'] = new Date(Date.now());
    if (qUpdating.push([sch, callback]) > 1) {    // an update is in progress
        return;
    }

    function _runUpdateQ() {
        let x = qUpdating.shift();
        if (x == undefined)
            return;
        sch = x[0];
        callback = x[1];

        Thenjs(function (cont) {
            genSchemaKey(sm, sch, cont);
        })
        .then(function (cont, arg) {
            sch.__key__ = arg;
            MongoOP.add(sm.collection, 0, 'lastid', 1, cont);
        })
        .then(function (cont, arg) {
            sch._id = arg.lastid;
            //console.log("name: %s, key: %d, id: %d", sch.__name__, sch.__key__, sch._id);
            sm.collection.insert({
                _id: sch._id,
                name: sch.__name__,
                key: sch.__key__,
                time: sch.__time__,
                who: sch.__who__,
                str: sch.__str__
            }, cont);
        })
        .then(function (cont, arg) {
            MongoOP.replaceMap(sm.collection, 0, 'schema', 'name',
                {name: sch.__name__, id: sch._id, who: sch.__who__, time: sch.__time__},
                false, cont);
        })
        .then(function (cont, arg) {
            addSchema(sm, sch, false);
            callback(null, sch);

            _runUpdateQ();
        })
        .fail(function (cont, err, result) {
            console.log("%s", err.stack);
            callback(err, null);

            _runUpdateQ();
        });
    }

    _runUpdateQ();
}

function update(data, who, callback) {
    let self = this;
    if ('function' == typeof who) {
        callback = who;
        who = os.hostname();
    }

    let schema = Parser.parse(data, self.SchemaByName);
    if (null == schema) {
        return process.nextTick(function () {
            callback('Invalid schema', null);
        });
    }
    //console.log(Parser.print(schema));

    let q = []
        , ret = 0
        , haserr = false
        , map = {};

    for (let x in schema) {
        let sch = schema[x];
        if (sch.__type__ == '__peter__') {
            sch.__who__ = who;
            // TODO: check compatibility
            q.push(sch);
        }
    }
    if (0 == q.length) {
        return process.nextTick(function () {
            callback('No peter specified', null);
        });
    }

    Thenjs.eachSeries(q, function (cont, each) {
        updateSchema(self, each, function (err, arg) {
            if (null != err)
                haserr = true;
            map[each.__name__] = arg;
            if (++ret == q.length) {
                callback(haserr ? 'Not all updated' : null, map);
            }
            else {
                cont();
            }
        });
    });
}

let __pp = Engine.generate(__dirname + '/schema.pp');
assert(null != __pp, "Failed to generate from schema.pp");
eval(__pp);

function _procUpdate(sm, arg) {
    function _run(data, callback) {
        console.log(data.toString('utf8'));
        sm.update(data, function (err, arg) {
            if (null == err) {
                console.log("Update succeeded!");
                for (let x in arg) {
                    console.log("Name: %s, Key: %d, Id: %d, Time: %s",
                        x, arg[x].__key__, arg[x]._id, timeString(arg[x].__time__));
                }
            }
            else {
                console.log("Update failed: %s", err.stack ? err.stack : err);
            }
            if (callback) {
                callback(err, arg);
            }
        });
    }

    if (arg.length >= 1) {
        let data = null;
        try {
            data = fs.readFileSync(arg[0]);
        }
        catch (e) {
            console.log(e.stack);
            if (undefined != arg[1]) {
                return process.nextTick(function () {
                    arg[1](e.toString(), 0);
                })
            }
            return null;
        }
        _run(data, arg[1]);
        return null;
    }

    let save = '';
    return function (str) {
        if (save != '')
            save += '\n';
        if (null != str) {
            save += str;
        }
        else {
            _run(Buffer.from(save));
        }
    };
}

function _procHistory(sm, name, key) {
    sm.collection.find({name: name, key: key}, {}).sort({_id: -1}).toArray(function (err, arg) {
        if (null == err) {
            for (let x in arg) {
                let inst = arg[x];
                console.log("// @@Create time: %s, by %s", timeString(inst.time), inst.who);
                console.log("// @@Version id: %d", inst._id);
                console.log(inst.str);
            }
        }
        else {
            console.log("Error: %s", err);
        }
    });
}

function _procSetAutoId(sch_name, from, callback) {
    let set = {};
    set[sch_name] = 'number' == typeof from ? from : parseInt(from);
    MongoOP.set(this.collection, 0, set, {}, callback);
}

function commandForMonitor(arg, rl) {
    let sch, cmd;
    let self = this;

    function _help() {
        console.log("Commands for schema:");
        console.log("help\tprint this message");
        console.log("list\tlist all schema");
        console.log("type\tprint parser type of a schema");
        console.log("    \targs: key or name");
        console.log("update\tupdate a new schema");
        console.log("      \targs: filename");
        console.log("hist\tlist all versions");
        console.log("    \targs: key or name");
        console.log("setautoid\tset the initial id");
        console.log("    \targs: schema_name id");
        console.log("You can also use 'schema `id|name'' to print a schema");
        return null;
    }

    function _whichSchema(str) {
        return ascii.isNumber(str)
            ? self.SchemaByKey[parseInt(str)]
            : self.SchemaByName[str];
    }

    function _printSchemaStr(sch) {
        console.log("// @@Create time: %s, by %s", timeString(sch.__time__), sch.__who__);
        console.log("// @@Version id: %d", sch.__id__);
        console.log(sch.__str__);
    }

    if (undefined == arg || arg.length < 1) {
        return _help();
    }
    cmd = arg.shift();
    switch (cmd) {
        case 'help':
            return _help();

        case 'list':
            console.log(sprintf("%24s%8s%8s%21s%30s", "NAME", "KEY", "ID", "TIME        ", "BY      "));
            console.log("=============================================================================================");
            for (let x in self.SchemaByKey) {
                sch = self.SchemaByKey[x];
                if (sch.hasOwnProperty('__key__')) {
                    console.log(sprintf("%24s%8d%8d%21s%30s", sch.__name__, sch.__key__,
                        sch.__id__, timeString(sch.__time__), sch.__who__));
                }
            }
            break;

        case 'type':
            if (arg.length < 1) {
                console.log("Key or name must be provided for 'type'");
                break;
            }
            cmd = arg.shift();
            sch = _whichSchema(cmd);
            if (undefined != sch) {
                console.log(Parser.print(sch));
            }
            else {
                console.log("Unknown schema: %s", cmd);
            }
            break;

        case 'hist':
        case 'history':
            if (arg.length < 1) {
                console.log("Key or name must be provided for 'hist'");
                break;
            }
            cmd = arg.shift();
            sch = _whichSchema(cmd);
            if (undefined != sch) {
                _procHistory(self, sch.__name__, sch.__key__);
            }
            else {
                console.log("Unknown schema: %s", cmd);
            }
            break;

        case 'update':
            return _procUpdate(self, arg);

        case 'setautoid':
            if (arg.length >= 2) {
                let sch = arg[0];
                let from = arg[1];
                let callback = ('function' == typeof arg[2])
                    ? arg[2]
                    : function (err, arg) {};
                return self.setAutoId(sch, from, callback);
            }
            return null;

        default:
            if (ascii.isNumber(cmd)) {
                rl.pause();
                MongoOP.get(self.collection, parseInt(cmd), function (err, arg) {
                    rl.resume();
                    if (null == err) {
                        console.log("// @@Create time: %s, by %s", timeString(arg.time), arg.who);
                        console.log("// @@Version id: %d", arg._id);
                        console.log(arg.str);
                    }
                    else {
                        console.log("Error: %s", err);
                    }
                });
                break;
            }
            sch = _whichSchema(cmd);
            if (undefined != sch) {
                _printSchemaStr(sch);
            }
            else if (cmd == 'all') {
                for (let x in self.SchemaByKey) {
                    _printSchemaStr(self.SchemaByKey[x]);
                }
            }
            else {
                console.log("Unknown command: %s", cmd);
            }
            break;
    }
    return null;
}

function _findToZip(sch, json) {
    if (undefined == sch.__ziplist__ || !json) {
        return null;
    }
    let zip = {};
    let out = [];
    let ret, name, flag = false;
    for (let x in sch.__ziplist__) {
        name = sch.__ziplist__[x];
        if (json.hasOwnProperty(name)) {
            let subsch = sch[name];
            let subjson = json[name];
            if (subsch.__zip__) {
                zip[name] = subjson;
                flag = true;
                delete json[name];
            }
            else {
                if (name.substr(0, 1) == '[') {
                    assert(subjson instanceof Array);
                    for (let x in subjson) {
                        ret = _findToZip(subsch, subjson[x]);
                        if (null != ret) {
                            out = out.concat(ret);
                        }
                    }
                }
                else {
                    ret = _findToZip(subsch, subjson);
                    if (null != ret) {
                        out = out.concat(ret);
                    }
                }
            }
        }
    }
    if (flag) {
        //console.log('To zip', sch.__name__, zip);
        out.push([json, zip]);
    }
    return out.length ? out : null;
}

function fake(str, callback) {
    return process.nextTick(function () {
        callback(null, str);
    });
}

function zipAny(sch, json, callback) {
    let out = _findToZip(sch, json);
    if (null == out) {
        return process.nextTick(function () {
            callback(null, json);
        });
    }
    //console.log(out);
    let n = out.length;
    for (let x in out) {
        function _zip(which) {
            zlib.deflate(JSON.stringify(which[1]), function (err, arg) {
                if (null == err) {
                    which[0].__zip__ = arg;
                }
                if (--n == 0) {
                    //console.log('ok');
                    callback(null, json);
                }
            });
        }
        _zip(out[x]);
    }
}

function _findZip(sch, json) {
    if (undefined == sch.__ziplist__ || !json) {
        return null;
    }
    let out = [];
    let ret, name;
    for (let x in sch.__ziplist__) {
        name = sch.__ziplist__[x];
        if (json.hasOwnProperty(name)) {
            let subsch = sch[name];
            let subjson = json[name];

            if (name.substr(0, 1) == '[') {
                assert(subjson instanceof Array);
                for (let x in subjson) {
                    ret = _findZip(subsch, subjson[x]);
                    if (null != ret) {
                        out = out.concat(ret);
                    }
                }
            }
            else {
                ret = _findZip(subsch, subjson);
                if (null != ret) {
                    out = out.concat(ret);
                }
            }
        }
    }
    if (json.__zip__) {
        out.push(json);
    }
    return out.length ? out : null;
}

function unzipAny(sch, json, callback) {
    assert(null != sch);
    let found = _findZip(sch, json);
    //console.log(found);
    if (null == found) {
        return process.nextTick(function () {
            callback(null, json);
        });
    }

    let n = found.length;
    for (let x in found) {
        function _unzip(which) {
            zlib.unzip(which.__zip__.buffer, function (err, arg) {
                assert(null == err);
                let unzip = JSON.parse(arg);
                for (let x in unzip) {
                    which[x] = unzip[x];
                }
                delete which.__zip__;

                if (--n == 0) {
                    callback(null, json);
                }
            });
        }
        _unzip(found[x]);
    }
}

function fill(sch, json) {
    let list, which;
    if (sch.__type__ == 'Object') {
        return json;
    }
    if (sch.hasOwnProperty('__defaultlist__')) {
        list = sch.__defaultlist__;
        for (let x in list) {
            which = list[x];
            if (!json.hasOwnProperty(which)) {
                if ('$NOW' == sch[which].__default__ && 'Time' == sch[which].__type__) {
                    json[which] = new Date();
                }
                else {
                    json[which] = sch[which].__default__;
                }
            }
        }
    }
    for (let x in json) {
        if (!sch[x])
            continue;
        switch (sch[x].__type__) {
        case '__list__':
        case '__set__':
        case '__keyed__':
        case '__link__':
            assert(json[x] instanceof Array);
            for (let y in json[x]) {
                fill(sch[x], json[x][y]);
            }
            break;

        case '__container__':
            fill(sch[x], json[x]);
            break;
        }
    }
    if (sch.hasOwnProperty('__inherit__')) {
        return fill(sch.__inherit__, json);
    }
    return json;
}

Manager.prototype = {
    update: update,
    loadFromDB: _loadFromDB,

    getByKey: function (key) {
        return this.SchemaByKey.hasOwnProperty(key)
            ? this.SchemaByKey[key]
            : null;
    },

    getByName: function (name) {
        return this.SchemaByName.hasOwnProperty(name)
            ? this.SchemaByName[name]
            : null;
    },

    getAllSchemaName: function () {
        let names = [];
        for (let x in this.SchemaByName) {
            names.push(x);
        }
        return names;
    },

    validate: function (name, json) {
        let sch = this.SchemaByName[name];
        if (null != sch && Parser.check(sch, json)) {
            return sch;
        }
        return null;
    },

    setAutoId: _procSetAutoId,
    command: commandForMonitor
};

module.exports = {
    createManager: function () {
        let mgr = new Manager();
        return mgr;
    },

    // populate default attributes
    fillDefault: function (sch, json, one) {
        switch (sch.__type__) {
        case '__list__':
        case '__set__':
        case '__keyed__':
        case '__link__':
            if (!one) {
                assert(json instanceof Array);
                for (let x in json) {
                    fill(sch, json[x]);
                }
                break;
            }
            // pass through
        default:
            return fill(sch, json);
        }
    },

    zipAny: zipAny,
    unzipAny: unzipAny
};

