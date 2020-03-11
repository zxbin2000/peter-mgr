/**
 * Created by linshiding on 3/10/15.
 */
let Promise = require("bluebird");
let Schema = require('./schema');
let Parser = require('./parser');
let MongoOP = require('./mongoop');
let utils = require('../utils/utils');
let Engine = require('../engine/engine');
let assert = require('assert');
let ObjectID = require('mongodb').ObjectID;
let BinaryParser = require('../utils/binary_parser').BinaryParser;
let fs = require('fs');
let sprintf = require('sprintf-js').sprintf;
let ascii = require('../utils/ascii');
let MongoClient = require('mongodb').MongoClient;
let pjson = require('../package.json');
let _ = require('lodash');

let VERSION = pjson.version;
let _key;

function Manager() {
    this.db = null;
    this.sm = Schema.createManager();
}

// this function is copied from mongodb/objectid.js, but modify the 3 bytes of machineId to schemaKey
ObjectID.prototype.generate = function (time) {
    // if ('number' != typeof time) {
    //     time = parseInt(Date.now() / 1000, 10);
    // }

    // let time4Bytes = BinaryParser.encodeInt(time, 32, true, true);
    // /* for time-based ObjectID the bytes following the time will be zeroed */
    // let schemaKey3Bytes = BinaryParser.encodeInt(_key, 24, true, true);
    // let pid2Bytes = BinaryParser.fromShort(typeof process === 'undefined' ? Math.floor(Math.random() * 100000) : process.pid % 0xFFFF);
    // let index3Bytes = BinaryParser.encodeInt(this.get_inc(), 24, false, true);
    // return time4Bytes + schemaKey3Bytes + pid2Bytes + index3Bytes;

    if ('number' !== typeof time) {
      time = ~~(Date.now() / 1000);
    }
  
    // Use pid
    var pid =
      (typeof process === 'undefined' || process.pid === 1
        ? Math.floor(Math.random() * 100000)
        : process.pid) % 0xffff;
    var inc = this.get_inc();
    // Buffer used
    var buffer = Buffer.alloc(12);
    // Encode time
    buffer[3] = time & 0xff;
    buffer[2] = (time >> 8) & 0xff;
    buffer[1] = (time >> 16) & 0xff;
    buffer[0] = (time >> 24) & 0xff;
    // Encode machine
    buffer[6] = _key & 0xff;
    buffer[5] = (_key >> 8) & 0xff;
    buffer[4] = (_key >> 16) & 0xff;
    // Encode pid
    buffer[8] = pid & 0xff;
    buffer[7] = (pid >> 8) & 0xff;
    // Encode index
    buffer[11] = inc & 0xff;
    buffer[10] = (inc >> 8) & 0xff;
    buffer[9] = (inc >> 16) & 0xff;
    // Return the buffer
    return buffer;
};

ObjectID.prototype.getSchemaKey = function () {
    if (undefined == this._key) {
        let buff = Buffer.from(this.toString(), 'hex');
        this._key = (buff[4] << 16) + (buff[5] << 8) + buff[6];
    }
    return this._key;
};

function collName(name) {
    let n = name.search(/\./);
    return -1 == n ? name : name.substring(0, n);
}

function _getCollection(pm, pid) {
    assert(pid instanceof ObjectID, 'invalid objectid of ' + pid);

    let key = pid.getSchemaKey();
    let sch = pm.sm.getByKey(key);

    assert(undefined != sch && null != sch);

    return pm.db.collection(collName(sch.__name__));
}

function genPeterId(key) {
    _key = key;
    let pid = new ObjectID();
    pid._key = key;
    return pid;
}

function genFromIntId(id) {
    if (Parser.isValidPeterId(id)) {
        return new ObjectID(id);
    }
    if ('string' == typeof id) {
        id = parseInt(id);
        assert(!isNaN(id));
    }
    return new ObjectID(sprintf('%024d', id));
}

function _create(id, name, json, options, callback) {
    if ('function' == typeof options) {
        callback = options;
        options = {};
    }

    let self = this;
    let sch = self.sm.validate(name, json);
    if (null == sch) {
        return process.nextTick(function () {
            callback('Schema check failed', name);
        });
    }
    Schema.fillDefault(sch, json);

    json['_id'] = genFromIntId(id);
    json['_schemaid'] = sch.__id__;

    Schema.zipAny(sch, json, function (err, arg) {
        MongoOP.create(self.db.collection(collName(name)), json, function (err, arg) {
            if (null != err) {
                if (11000 == err.code) {
                    if (options.update) {
                        id = json._id;
                        delete json._id;
                        delete json._schemaid;
                        return MongoOP.set(self.db.collection(collName(name)), id, json, {}, function (err, arg) {
                            callback(err, null==err ? id : arg);
                        });
                    }
                    return callback('Already existed', 0);
                }
                return callback(err, 0);
            }
            callback(null, id);
        });
    });
}

function create(name, json, callback) {
    assert(typeof json === 'object', 'Invalid parameters');
    
    let self = this;
    let sch = self.sm.validate(name, json);
    if (null == sch) {
        return process.nextTick(function () {
            callback('Schema check error: ' + name, null);
        });
    }
    Schema.fillDefault(sch, json);

    let id = genPeterId(sch.__key__);
    json['_id'] = id;
    json['_schemaid'] = sch.__id__;

    Schema.zipAny(sch, json, function (err, arg) {
        MongoOP.create(self.db.collection(collName(name)), json, function (err, arg) {
            callback(err, null == err ? id : arg);
        });
    });
}

function createS(name, json, callback) {
    let self = this;
    let sch = self.sm.validate(name, json);
    if (null == sch) {
        return process.nextTick(function () {
            callback('Schema check error: ' + name, null);
        });
    }
    Schema.fillDefault(sch, json);

    MongoOP.increase(self.db.collection('_schema'), 0, name, 1, function (err, arg) {
        if (null != err) {
            return callback(err, arg);
        }

        let id = arg[name];
        let str = sprintf("%024d", id);
        json['_id'] = new ObjectID(str);
        json['_schemaid'] = sch.__id__;

        Schema.zipAny(sch, json, function (err, arg) {
            MongoOP.create(self.db.collection(collName(name)), json, function (err, arg) {
                if (null != err) {
                    if (11000 == err.code) {
                        // maybe some lagacy doc exists, do it again
                        delete json['_id'];
                        delete json['_schemaid'];
                        return self.createS(name, json, callback);
                    }
                    return callback(err, arg);
                }
                callback(null, id);
            });
        });
    });
}

function isExpectedPeter(pid, name) {
    if ('string' === typeof pid && Parser.isValidPeterId(pid)) {
        pid = new ObjectID(pid);
    }

    if (pid instanceof ObjectID) {
        let key = pid.getSchemaKey();
        let sch = this.sm.getByKey(key);
        return sch && sch.__name__ === name;
    }

    return false;
}

function _checkPid(sm, pid, callback) {
    if (pid instanceof ObjectID) {
        return pid;
    }
    if (Parser.isValidPeterId(pid)) {
        return new ObjectID(pid);
    }
    if ('string' === typeof pid) {
        let n = pid.search(/\./);
        if (-1 != n) {
            let schname = pid.substring(0, n);
            let id = pid.substring(n + 1, pid.length);
            let sch = sm.getByName(schname);
            if (null != sch) {
                pid = Parser.isValidPeterId(id)
                    ? new ObjectID(id)
                    : new ObjectID(sprintf('%024d', parseInt(id)));
                pid._key = sch.__key__;
                return pid;
            }
        }
    }
    process.nextTick(function () {
        callback("Invalid pid '" + pid + "' provided", null);
    });
    return null;
}

function _checkSchemaAndCallback(sm, pid, attrname, json, callback) {
    pid = _checkPid(sm, pid, callback);
    if (null == pid)
        return;

    let sch = sm.getByKey(pid.getSchemaKey());
    if (null == sch) {
        process.nextTick(function () {
            callback("Invalid pid '" + pid + "' provided", null);
        });
        return null;
    }

    let attr = Parser.findAttribute(sch, attrname);
    if (null == attr) {
        process.nextTick(function () {
            callback("Invalid attr name '" + attrname + "' provided", null);
        });
        return null;
    }

    if (null != json && !Parser.checkElement(attr, json)) {
        process.nextTick(function () {
            callback("Schema '" + sch.__name__ + '.' + attrname + "' check fail: " + JSON.stringify(json), null);
        });
        return null;
    }
    return [pid, attr, sch];
}

function destroy(pid, options, callback) {
    if('function' == typeof options) {
        callback = options;
        options = {};
    }
    assert('function' == typeof callback, 'Invalid parameters');

    let self = this;
    pid = _checkPid(self.sm, pid, callback);
    if (null == pid)
        return;

    MongoOP.destroy(_getCollection(self, pid), { _id: pid }, options, function (err, arg) {
        callback(err, null == err ? arg.result : arg);
    });
}

function unzipAny(sch, arg, options, callback) {
    if (false != options.unzip) {
        //let sch = self.sm.getByKey(pid.getSchemaKey());
        Schema.unzipAny(sch, arg, callback);
    }
    else {
        callback(null, arg);
    }
}

// fields is optional, it can be an attribute name or an array of attributes
function get(pid, fields, options, callback) {
    let self = this;
    if ('function' == typeof fields) {
        callback = fields;
        fields = {};
        options = {};
    }
    if ('function' == typeof options) {
        callback = options;
        options = {};
    }
    pid = _checkPid(self.sm, pid, callback);
    if (null == pid)
        return;

    MongoOP.get(_getCollection(self, pid), pid, fields, function (err, arg) {
        if (null == err) {
            unzipAny(self.sm.getByKey(pid.getSchemaKey()), arg, options, callback);
        } else if(err === 'Not existing' || err === 'No such fields') {
            callback(null, null);
        } else {
            callback(err, arg);
        }
    });
}

function increase(pid, name, step, callback) {
    let self = this;
    if ('function' == typeof step) {
        callback = step;
        step = 1;
    }
    let ret = _checkSchemaAndCallback(self.sm, pid, name, null, callback);
    if (null == ret)
        return;
    pid = ret[0];
    let attr = ret[1];

    if (attr.__type__ != 'Integer' && attr.__type__ != 'Number') {
        return process.nextTick(callback, name + ' is not integer.', null);
    }

    return MongoOP.increase(_getCollection(self, pid), pid, name, step, callback);
}

// fields can be an attribute name or an array of attributes
function remove(pid, fields, callback) {
    assert('function' == typeof callback);

    let self = this;
    pid = _checkPid(self.sm, pid, callback);
    if (null == pid)
        return;

    MongoOP.remove(_getCollection(self, pid), pid, fields, callback);
}

function manyGet(pids, fields, options, callback) {
    assert(pids instanceof Array);
    if (undefined == callback) {
        if (undefined == options) {
            assert('function' == typeof fields);
            callback = fields;
            fields = null;
            options = {};
        }
        else {
            assert('function' == typeof options);
            callback = options;
            if (fields instanceof Array || 'string' == typeof fields) {
                options = {};
            }
            else {
                options = fields ? fields : {};
                fields = null;
            }
        }
    }

    let newpids = []
        , key
        , pid
        , self = this;

    if (0 == pids.length) {
        return process.nextTick(function () {
            callback(null, []);
        });
    }
    for (let i = 0; i < pids.length; i++) {
        pid = pids[i];
        if (null == (pid = _checkPid(self.sm, pid, callback)))
            return;
        if (0 == i) {
            key = pid.getSchemaKey();
        }
        else if (key != pid.getSchemaKey()) {
            return process.nextTick(function () {
                callback("Not same schema of pid '" + pid + "'.", null);
            });
        }
        newpids.push(pid);
    }

    MongoOP.manyGet(_getCollection(self, pid), newpids, fields, function (err, arg) {
        if (!err && false != options.unzip) {
            let n = 0;
            let sch = self.sm.getByKey(key);

            for (let x in arg) {
                n ++;
                Schema.unzipAny(sch, arg[x], function (err, ret) {
                    if (--n == 0) {
                        callback(null, arg);
                    }
                });
            }
            if (0 != n)
                return;
        }
        callback(err, arg);
    });
}

// options can be {upsert: true, cond: {}}
function set(pid, json, options, callback) {
    if ('function' == typeof options) {
        callback = options;
        options = {};
    }

    let self = this;
    pid = _checkPid(self.sm, pid, callback);
    if (null == pid)
        return;

    let sch = self.sm.getByKey(pid.getSchemaKey());
    if (null == sch) {
        return process.nextTick(function () {
            callback("Invalid pid '" + pid + "' provided", null);
        });
    }

    if (null != json && !Parser.checkElement(sch, json)) {
        return process.nextTick(function () {
            callback("Schema '" + sch.__name__ + "' check fail: " + JSON.stringify(json), null);
        });
    }

    let attr, elem;
    for (let x in json) {
        elem = json[x];
        attr = Parser.findAttribute(sch, x);
        if (null == attr)       // should be __zip__, otherwise it will not pass Parser.checkElement
            continue;
        if (attr.__zip__ && 1!=sch.__ziplist__.length) {
            return process.nextTick(function () {
                callback("Zipped member '" + attr.__name__ + "' can't be set.", null);
            });
        }
        Schema.fillDefault(attr, elem);
    }
    Schema.zipAny(sch, json, function (err, arg) {
        MongoOP.set(_getCollection(self, pid), pid, json, options, callback);
    });
}

// options can be ??
function replace(pid, name, value, options, callback) {
    if ('function' == typeof options) {
        callback = options;
        options = {};
    }

    let self = this;
    let ret = _checkSchemaAndCallback(self.sm, pid, name, value, callback);
    if (null == ret)
        return;
    pid = ret[0];
    let attr = ret[1];
    Schema.fillDefault(attr, value, true);

    let sch = ret[2];
    if (attr.__zip__ && 1 != sch.__ziplist__.length) {
        return process.nextTick(function () {
            callback("Zipped member '" + sch.__name__ + "' can't be replaced.", null);
        });
    }

    let json = {};
    json[name] = value;
    Schema.zipAny(sch, json, function (err, arg) {
        MongoOP.replace(_getCollection(self, pid), pid, name, json, options, callback);
    });
}

// options can be {upsert: true}
function insert(pid, name, value, options, callback) {
    if ('function' == typeof options) {
        callback = options;
        options = {};
    }

    let self = this;
    let ret = _checkSchemaAndCallback(self.sm, pid, name, value, callback);
    if (null == ret)
        return;
    pid = ret[0];
    let attr = ret[1];
    Schema.fillDefault(attr, value, true);

    let sch = ret[2];
    if (attr.__zip__ && 1!=sch.__ziplist__.length) {
        return process.nextTick(function () {
            callback("Zipped member '" + sch.__name__ + "' can't be inserted.", null);
        });
    }

    let json = {};
    json[name] = value;
    Schema.zipAny(sch, json, function (err, arg) {
        MongoOP.insert(_getCollection(self, pid), pid, name, json, callback);
    });
}

function _findLink(sch1, sch2, linkname, callback) {
    let attr1, attr2;
    let link1, link2;

    if ('~' == linkname) {
        link1 = '~' + sch2.__name__;
        link2 = '~' + sch1.__name__;
    }
    else if (linkname == '~' + sch2.__name__) {
        link1 = linkname;
        link2 = '~' + sch1.__name__;
    }
    else {
        link1 = link2 = linkname;
    }

    attr1 = Parser.findAttribute(sch1, link1);
    if (null == attr1 || '__link__' != attr1.__type__) {
        process.nextTick(function () {
            callback("Invalid attr name '" + link1 + "' provided", null);
        });
        return null;
    }
    attr2 = Parser.findAttribute(sch2, link2);
    if (null == attr2 || '__link__' != attr2.__type__) {
        //console.log("2\n" + attr1);
        process.nextTick(function () {
            callback("Invalid attr name '" + link2 + "' provided", null);
        });
        return null;
    }

    if (attr1.__to__ == attr2) {
        return [link1, link2, attr1, attr2];
    }

    process.nextTick(function () {
        callback("Link not specified in schema", null);
    });
    return null;
}

function _checkLink(sm, pid1, pid2, linkname, callback) {
    let sch1, sch2;

    pid1 = _checkPid(sm, pid1, callback);
    if (null == pid1)
        return null;
    pid2 = _checkPid(sm, pid2, callback);
    if (null == pid2)
        return null;

    if (pid1.equals(pid2) && pid1._key == pid2._key) {
        process.nextTick(function () {
            callback("Self...", null);
        });
        return null;
    }

    // get both schemas
    sch1 = sm.getByKey(pid1.getSchemaKey());
    if (null == sch1) {
        process.nextTick(function () {
            callback("Invalid pid '" + pid1 + "' provided", null);
        });
        return null;
    }
    sch2 = sm.getByKey(pid2.getSchemaKey());
    if (null == sch2) {
        process.nextTick(function () {
            callback("Invalid pid '" + pid2 + "' provided", null);
        });
        return null;
    }

    let ret = _findLink(sch1, sch2, linkname, callback);
    if (null == ret)
        return null;

    return [pid1, pid2, ret[0], ret[1], ret[2], ret[3]];

    /*
     if ('~' == linkname) {
     link1 = '~' + sch2.__name__;
     link2 = '~' + sch1.__name__;
     }
     else if (linkname == '~' + sch2.__name__) {
     link1 = linkname;
     link2 = '~' + sch1.__name__;
     }
     else {
     link1 = link2 = linkname;
     }

     attr1 = Parser.findAttribute(sch1, link1);
     if (null == attr1 || '__link__' != attr1.__type__) {
     process.nextTick(function () {
     callback("Invalid attr name '" + link1 + "' provided", null);
     });
     return null;
     }
     attr2 = Parser.findAttribute(sch2, link2);
     if (null == attr2 || '__link__' != attr2.__type__) {
     process.nextTick(function () {
     callback("Invalid attr name '" + link2 + "' provided", null);
     });
     return null;
     }

     if (attr1.peer.__element__ == sch2.__name__ && attr2.peer.__element__ == sch1.__name__) {
     return [pid1, pid2, link1, link2, attr1, attr2];
     }

     process.nextTick(function () {
     callback("Link not specified in schema", null);
     });
     return null;*/
}

function _procAtt(attr, att, pid, now, callback) {
    if (null != att) {
        if (!Parser.checkElement(attr, att)) {
            process.nextTick(function () {
                callback("att1 is invalid", null);
            });
            return null;
        }
    }
    else {
        att = {};
    }
    att['peer'] = pid;
    att['time'] = now;
    return Schema.fillDefault(attr, att, true);
}

// att1 and att2 is optional
function link(pid1, pid2, linkname, att1, att2, callback) {
    let self = this;
    if ('function' === typeof att1) {
        callback = att1;
        att1 = null;
        att2 = null;
    }
    else if ('function' == typeof att2) {
        callback = att2;
        att2 = null;
    }

    let ret = _checkLink(self.sm, pid1, pid2, linkname, callback);
    if (null == ret)
        return;

    pid1 = ret[0];
    pid2 = ret[1];
    let link1 = ret[2];
    let link2 = ret[3];
    let attr1 = ret[4];
    let attr2 = ret[5];

    let now = new Date(Date.now());
    if (null == (att1 = _procAtt(attr1, att1, pid2, now, callback))
    || (null == (att2 = _procAtt(attr2, att2, pid1, now, callback))))
        return;

    // TODO: coregion
    // doLink is defined in peter.pp
    doLink(self, pid1, pid2, link1, link2, att1, att2, callback);
}

function unlink(pid1, pid2, linkname, callback) {
    let self = this;
    let ret = _checkLink(self.sm, pid1, pid2, linkname, callback);
    if (null == ret)
        return;

    pid1 = ret[0];
    pid2 = ret[1];
    let link1 = ret[2];
    let link2 = ret[3];

    // TODO: coregion
    MongoOP.removeMap(_getCollection(self, pid1), pid1, link1, 'peer', pid2, function (err, arg) {
        MongoOP.removeMap(_getCollection(self, pid2), pid2, link2, 'peer', pid1, function (err, arg) {
            callback(err, arg);
        });
    });
}

function isLinked(pid1, pid2, linkname, callback) {
    let self = this;
    let ret = _checkLink(self.sm, pid1, pid2, linkname, callback);
    if (null == ret)
        return;

    pid1 = ret[0];
    pid2 = ret[1];
    let link1 = ret[2];

    MongoOP.getElementsByCond(_getCollection(self, pid1), pid1, link1, {peer: pid2}, function (err, arg) {
        callback(err, null == err ? arg[0].time : arg);
    });
}

function getLinks(pid, linkname, callback) {
    let self = this;
    let ret = _checkSchemaAndCallback(self.sm, pid, linkname, null, callback);
    if (null == ret)
        return;

    pid = ret[0];
    MongoOP.get(_getCollection(self, pid), pid, linkname, callback);
}

function push(pid, listname, elem, update, callback) {
    if('function' === typeof update) {
      callback = update;
      update = false;
    }
    assert('boolean' === typeof update, 'Invalid parameter');
    assert(callback, 'callback cannot be null.');

    let self = this;
    let ret = _checkSchemaAndCallback(self.sm, pid, listname, elem, callback);
    if (null == ret)
        return;
    let _id = ret[0];
    let attr = ret[1];
    Schema.fillDefault(attr, elem, true);
    let collection = _getCollection(self, _id);

    Schema.zipAny(attr, elem, function (err, arg) {
        switch (attr.__type__) {
        case '__list__':
            MongoOP.pushList(collection, _id, listname, elem, function(err, arg) {
              callback(err, arg.result ? arg.result : arg);
            });
            break;

        case '__keyed__':
            MongoOP.pushMap(collection, _id, listname, attr.__key__, elem, function (err, arg) {
                if (null == err) {
                    return callback(null, arg.result);
                }
                if ('Already existed' == err) {
                    if(update) {
                      return MongoOP.replaceMap(collection, _id, listname, attr.__key__, elem, true, function(err, arg) {
                        callback(err, arg.result ? arg.result : arg);
                      });
                    } else {
                      return callback(null, { errcode: err });
                    }
                }
                callback(err, arg.result);
            });
            break;

        case '__set__':
            MongoOP.pushSet(collection, _id, listname, elem, function(err, arg) {
              process.nextTick(function () {
                callback(err, arg.result ? arg.result : arg);
              });
            });
            break;

        default:
            process.nextTick(function () {
                callback("'" + listname + "' is not a container", null);
            });
            break;
        }
    });
}

function pop(pid, setname, first, callback) {
    let self = this;
    if('function' === typeof first) {
        callback = first;
        first = false;
    }
    assert('function' === typeof callback, 'callback cannot be null.');

    let ret = _checkSchemaAndCallback(self.sm, pid, setname, null, callback);
    if (null == ret)
        return;
    pid = ret[0];
    let attr = ret[1];

    switch (attr.__type__) {
        case '__list__':
        case '__set__':
        case '__keyed__':
            MongoOP.pop(_getCollection(self, pid), pid, setname, first, function(err, arg) {
                if('Not existing' === err) {
                    return callback(null, { errcode: err });
                } else {
                    return callback(err, arg);
                }
            });
            break;
        default:
            process.nextTick(function () {
                callback("'" + setname + "' is not a container", null);
            });
            break;
    }
}

function _checkSetSchemaAndCallback(sm, pid, setname, json, callback) {
    let ret = _checkSchemaAndCallback(sm, pid, setname, json, callback);
    if (null == ret)
        return;
    let attr = ret[1];

    switch (attr.__type__) {
        case '__list__':
            process.nextTick(function () {
                callback("'" + setname + "' is not a set", null);
            });
            return null;

        case '__set__':
        case '__keyed__':
            return ret;
    }

    process.nextTick(function () {
        callback("'" + setname + "' is not a container", null);
    });
    return null;
}

function replaceElementByKey(pid, setname, elem, replaceAll, callback) {
    let self = this;
    if (undefined == callback) {
        assert('function' === typeof replaceAll);
        callback = replaceAll;
        replaceAll = false;
    }

    let ret = _checkSetSchemaAndCallback(self.sm, pid, setname, elem, callback);
    if (null == ret)
        return;
    pid = ret[0];
    let attr = ret[1];
    if (replaceAll) {
        Schema.fillDefault(attr, elem, true);
    }

    let keyname = attr.__key__;
    assert(keyname != '', "keyname != '' in replaceElementByKey");
    Schema.zipAny(attr, elem, function (err, arg) {
        MongoOP.replaceMap(_getCollection(self, pid), pid, setname, keyname, elem, replaceAll, callback);
    });
}

function replaceElement(pid, setname, old, _new, callback) {
    let self = this;
    let ret = _checkSetSchemaAndCallback(self.sm, pid, setname, _new, callback);
    if (null == ret)
        return;
    pid = ret[0];
    let attr = ret[1];
    Schema.fillDefault(attr, _new, true);

    assert(attr.__key__ == '', "keyname == '' in replaceElement");
    MongoOP.replaceSet(_getCollection(self, pid), pid, setname, old, _new, callback);
}

function replaceElementByIndex(pid, contname, index, _new, replaceAll, callback) {
    let self = this;
    if (undefined == callback) {
        assert('function' === typeof replaceAll);
        callback = replaceAll;
        replaceAll = false;
    }

    let ret = _checkSchemaAndCallback(self.sm, pid, contname, _new, callback);
    if (null == ret)
        return;
    pid = ret[0];
    let attr = ret[1];
    if (replaceAll) {
        Schema.fillDefault(attr, _new, true);
    }

    MongoOP.replaceElementByIndex(_getCollection(self, pid), pid, contname, index, _new, replaceAll, callback);
}

function removeElement(pid, setname, elem, callback) {
    let self = this;
    let ret = _checkSetSchemaAndCallback(self.sm, pid, setname, elem, callback);

    if (null == ret)
        return;
    pid = ret[0];
    let attr = ret[1];

    MongoOP.removeSet(_getCollection(self, pid), pid, setname, elem, function(err, arg) {
        if(err === 'Not existing') {
            return callback(null, { errcode: err });
        } else {
            return callback(err, (arg && arg.result) ? arg.result : arg);
        }
    });
}

function removeElementsByCond(pid, cont_name, cond, callback) {
    let self = this;
    let ret = _checkSetSchemaAndCallback(self.sm, pid, cont_name, null, callback);
    if (null == ret)
        return;
    pid = ret[0];

    MongoOP.removeElementsByCond(_getCollection(self, pid), pid, cont_name, cond, function(err, arg) {
      if(err === 'Not existing') {
        return callback(null, { errcode: err });
      } else {
        return callback(err, (arg && arg.result) ? arg.result : arg);
      }
    });
}

function removeElementByKey(pid, setname, key, callback) {
    let self = this;
    let ret = _checkSetSchemaAndCallback(self.sm, pid, setname, null, callback);
    if (null == ret)
        return;
    pid = ret[0];
    let attr = ret[1];

    assert(attr.__key__ != '', "keyname != '' in removeElementByKey");
    if (attr[attr.__key__].__type__ == 'Integer')
        key = +key;
    MongoOP.removeMap(_getCollection(self, pid), pid, setname, attr.__key__, key, function(err, arg) {
        if(err === 'Not existing') {
          return callback(null, { errcode: err });
        } else {
          return callback(err, (arg && arg.result) ? arg.result : arg);
        }
    });
}

function getElementByKey(pid, setname, key, options, callback) {
    assert(key, 'key must be provided.');
    let self = this;
    if (undefined == callback) {
        callback = options;
        options = {};
    }

    let ret = _checkSetSchemaAndCallback(self.sm, pid, setname, null, callback);
    if (null == ret)
        return;
    pid = ret[0];
    let attr = ret[1];
    let keyname = attr.__key__;
    assert(keyname != '');
    if (attr[attr.__key__].__type__ == 'Integer')
        key = +key;

    if (attr.__zip__) {
        return MongoOP.get(_getCollection(self, pid), pid, '__zip__', function (err, arg) {
            if (null == err) {
                return unzipAny(self.sm.getByKey(pid.getSchemaKey()), arg, options, function (err, arg) {
                    arg = arg[setname];
                    for (let x in arg) {
                        if (arg[keyname] == key) {
                            return callback(null, arg[keyname]);
                        }
                    }
                    return callback(options.graceful ? null : 'Not existing', null);
                });
            }
            if ('Not existing' == err) {
                return callback(null, { errcode: err });
            } else {
              return callback(err, (arg && arg.result) ? arg.result : arg);
            }
        });
    }

    let cond = {};
    cond[keyname] = key;
    MongoOP.getElementsByCond(_getCollection(self, pid), pid, setname, cond, function (err, arg) {
        if (null == err) {
            return unzipAny(self.sm.getByKey(pid.getSchemaKey()), arg[0], options, callback);
        }
        if ('Not existing' == err) {
            return callback(null, { errcode: err });
        } else {
            return callback(err, (arg && arg.result) ? arg.result : arg);
        }
    });
}

function getElementByIndex(pid, listname, index, callback) {
    let self = this;
    let ret = _checkSchemaAndCallback(self.sm, pid, listname, null, callback);
    if (null == ret)
        return;
    pid = ret[0];
    let attr = ret[1];

    // TODO::
}

function getElementsByRange(pid, cont_name, range, callback) {
    let self = this;
    let ret = _checkSetSchemaAndCallback(self.sm, pid, cont_name, null, callback);
    if (null == ret)
        return;
    pid = ret[0];

    MongoOP.getElementsByRange(_getCollection(self, pid), pid, cont_name, range, callback);
}

function _procCreate(pm, arg) {
    let name = arg.shift();
    if (undefined == name) {
        console.log("Schema name must be provided for 'create'");
        return null;
    }
    let sch = pm.sm.getByName(name);
    if (null == sch) {
        console.log("Invalid schema name %s", name);
        return null;
    }

    let file = arg.shift()
        , save = '';

    if (undefined != file) {
        try {
            let data = fs.readFileSync(file);
            save = data.toString();
        }
        catch (e) {
            console.log(e);
            return null;
        }
    }

    function _run() {
        let json;
        try {
            json = new Function('return ' + save + ';')();
            if (null == json || undefined == json)
                return;
        }
        catch (e) {
            console.log(e);
            return;
        }

        console.log("create('" + name + "', " + save + ");");
        pm.create(name, json, function (err, arg) {
            if (null == err) {
                console.log("Succeed. id: %s", arg);
            }
            else {
                console.log("Error: %s", err);
            }
        });
    }

    if ('' != save) {
        _run();
        return null;
    }
    return function (str) {
        if (save != '')
            save += ' ';
        if (null != str) {
            save += str;
        }
        else {
            _run();
        }
    };
}

function _procSet(pm, arg) {
    let pidstr = arg.shift();
    if (undefined == pidstr) {
        console.log("Peter id must be provided for 'set'");
        return null;
    }
    try {
        let pid = new ObjectID(pidstr);
    }
    catch (e) {
        console.log(e);
        return null;
    }
    let sch = pm.sm.getByKey(pid.getSchemaKey());
    if (null == sch) {
        console.log("Invalid pid %s", pidstr);
        return null;
    }

    let file = arg.shift()
        , save = '';

    if (undefined != file) {
        try {
            let data = fs.readFileSync(file);
            save = data.toString();
        }
        catch (e) {
            console.log(e);
            return null;
        }
    }

    function _run() {
        let json;
        try {
            json = new Function('return ' + save + ';')();
            if (null == json || undefined == json)
                return;
        }
        catch (e) {
            console.log(e);
            return;
        }

        console.log("set('" + pidstr + "', " + save + ");");
        pm.set(pid, json, function (err, arg) {
            if (null == err) {
                console.log("Succeed. id: %s", arg);
            }
            else {
                console.log("Error: %s", err);
            }
        });
    }

    if ('' != save) {
        _run();
        return null;
    }
    return function (str) {
        if (save != '')
            save += ' ';
        if (null != str) {
            save += str;
        }
        else {
            _run();
        }
    };
}

function _procLinkUnlink(pm, cmd, arg) {
    if (3 != arg.length) {
        console.log('%s pid1 pid2 name', cmd);
        return null;
    }

    let func = eval(cmd).bind(pm);
    func(arg[0], arg[1], arg[2], function (err, arg) {
        if (null == err) {
            console.log("Succeeded! " + arg);
        }
        else {
            console.log("Error: %s", err);
        }
    });
    return null;
}

let pp = Engine.generate(__dirname + '/peter.pp');
assert(null != pp, "Failed to generate from peter.pp");
eval(pp);

function command4Monitor(arg, rl) {
    let cmd;
    let self = this;

    function _help() {
        console.log("Commands for peter:");
        console.log("help\tprint this message");
        console.log("create\tcreate a peter");
        console.log("    \targs: name [file]");
        console.log("    \tif no file provided, read json string from stdin");
        console.log("You can also use 'peter `id'' to retrieve a peter");
        return null;
    }

    if (undefined == arg || arg.length < 1) {
        return _help();
    }
    cmd = arg.shift();
    switch (cmd) {
        case 'help':
            return _help();

        case 'create':
            return _procCreate(self, arg);

        case 'set':
            return _procSet(self, arg);

        case '!clear':
            if (0==arg.length || '-f'!=arg[0]) {
                console.log('It is a very dangerous command. It will drop all collections.');
                console.log('If you know what you are doing, please add -f flag to run again.');
                return null;
            }
            _procClear(self, function (err, arg) {
                console.log('ok');
            });
            break;

        case 'link':
        case 'unlink':
        case 'isLinked':
            return _procLinkUnlink(self, cmd, arg);

        default:
            let pid = null;

            try {
                pid = ObjectID(cmd);
            }
            catch (e) {
                //console.log(e.toString());
                console.log("'%s' is not a valid pid", cmd);
                return null;
            }
            self.get(pid, arg[0], function (err, arg) {
                if (null == err) {
                    console.log("// @@Create time: %s", utils.timeString(pid.getTimestamp()));
                    console.log("// @@Schema: %s, id: %d", self.sm.getByKey(pid.getSchemaKey()).__name__, arg._schemaid);
                    console.log(arg);
                }
                else {
                    console.log("Error: %s", err);
                }
            });
            break;
    }
    return null;
}

function find(collName, cond, options, callback) {
    let self = this;
    let collection = self.db.collection(collName);
    if ('function' === typeof cond) {
        callback = cond;
        cond = {};
        options = {};
    }
    if ('function' === typeof options) {
        callback = options;
        options = {};
    }
    MongoOP.find(collection, cond, options, function (err, arg) {
        if (!err && false != options.unzip) {
            let n = 0;
            let sch = self.sm.getByName(collName);
            for (let x in arg) {
                n ++;
                Schema.unzipAny(sch, arg[x], function (err, ret) {
                    if (--n == 0) {
                        callback(null, arg);
                    }
                });
            }
            if (0 != n)
                return;
        }
        callback(err, arg);
    });
}

function findOne(collName, cond, options, callback) {
    let self = this;
    let collection = self.db.collection(collName);
    if ('function' == typeof cond) {
        callback = cond;
        cond = {};
        options = {};
    }
    if ('function' == typeof options) {
        callback = options;
        options = {};
    }
    MongoOP.findOne(collection, cond, options, function (err, arg) {
        if (!err && false != options.unzip) {
            let n = 0;
            let sch = self.sm.getByName(collName);
            for (let x in arg) {
                n ++;
                Schema.unzipAny(sch, arg[x], function (err, ret) {
                    if (--n == 0) {
                        callback(null, arg);
                    }
                });
            }
            if (0 != n)
                return;
        }
        callback(err, arg);
    });
}

function findOneAndUpdate(collName, filter, update, options, callback) {
    if('function' == typeof(options)) {
      callback = options;
      options = {};
    }
    assert('object' == typeof filter, 'Invalid parameter: filter');
    assert('object' == typeof update, 'Invalid parameter: update');
    assert('function' == typeof callback, 'callback is not a function');
    
    let self = this;
    let operators = {};
    let $set = {};
    for(let prop in update) {
        if(update.hasOwnProperty(prop) && !prop.startsWith('$')) {
            $set[prop] = update[prop];
        } else if(prop == '$set') {
            $set = update[prop];
        } else {
            operators[prop] = update[prop];
        }
    }
    
    if(Object.keys($set).length > 0) {
        if(options.upsert) {
            let sch = self.sm.validate(collName, $set);
            if (null == sch) {
                return process.nextTick(function () {
                    callback('Schema check error: ' + collName, null);
                });
            }
            Schema.fillDefault(sch, $set);
            operators['$setOnInsert'] = { 
                _id: genPeterId(sch.__key__),
                _schemaid: sch.__id__
            };
            options['returnOriginal'] = false;
        } else {
            let sch = self.sm.getByName(collName);
            if (!Parser.checkElement(sch, $set)) {
                return process.nextTick(function () {
                    callback("Schema '" + sch.__name__ + "' check fail: " + JSON.stringify(json), null);
                });
            }
            options['returnOriginal'] = false;
        }
        operators['$set'] = $set;
    }
    
    MongoOP.findOneAndUpdate(self.db.collection(collName), filter, operators, options, function (err, arg) {
      if (!err) {
            let n = 0;
            let sch = self.sm.getByName(collName);
            for (let x in arg) {
                n ++;
                Schema.unzipAny(sch, arg[x], function (err, ret) {
                    if (--n == 0) {
                        callback(null, arg);
                    }
                });
            }
            if (0 != n)
                return;
        }
        callback(err, arg);
    });
}

function findOneAndDelete(collName, filter, options, callback) {
    let self = this;
    let collection = self.db.collection(collName);
    if('function' == typeof(options)) {
      callback = options;
      options = {};
    }
    MongoOP.findOneAndDelete(collection, filter, options, function (err, arg) {
        if (!err) {
            let n = 0;
            let sch = self.sm.getByName(collName);
            for (let x in arg) {
                n ++;
                Schema.unzipAny(sch, arg[x], function (err, ret) {
                    if (--n == 0) {
                        callback(null, arg);
                    }
                });
            }
            if (0 != n)
                return;
        }
        callback(err, arg);
    });
}

function findOneAndReplace(collName, filter, replacement, options, callback) {
    let self = this;
    let collection = self.db.collection(collName);
    if('function' == typeof(options)) {
      callback = options;
      options = {};
    }
    let sch = self.sm.validate(collName, replacement);
    if (null == sch) {
        return process.nextTick(function () {
            callback('Schema check error: ' + collName, null);
        });
    }
    Schema.fillDefault(sch, replacement);
    replacement._schemaid = sch.__id__;
    options.returnOriginal = false;
    MongoOP.findOneAndReplace(collection, filter, replacement, options, function (err, arg) {
        if (!err) {
            let n = 0;
            let sch = self.sm.getByName(collName);
            for (let x in arg) {
                n ++;
                Schema.unzipAny(sch, arg[x], function (err, ret) {
                    if (--n == 0) {
                        callback(null, arg);
                    }
                });
            }
            if (0 != n)
                return;
        }
        callback(err, arg);
    });
}

function aggregate(collName, cond, options, callback) {
    let self = this;
    let collection = self.db.collection(collName);
    if ('function' == typeof options) {
        callback = options;
        options = {};
    }
    assert('function' == typeof callback, 'callback is not a function');
    MongoOP.aggregate(collection, cond, options, function(err, arg) {
        if (!err && false != options.unzip) {
            let n = 0;
            let sch = self.sm.getByName(collName);
            for (let x in arg) {
                n ++;
                Schema.unzipAny(sch, arg[x], function (err, ret) {
                    if (--n == 0) {
                        callback(null, arg);
                    }
                });
            }
            if (0 != n)
                return;
        }
        callback(err, arg);
    });
}

function count(collName, cond, callback) {
    let collection = this.db.collection(collName);
    MongoOP.count(collection, cond, function (err, arg) {
        callback(err, arg);
    });
}

function distinct(collName, field, cond, options, callback) {
    let self = this;
    let collection = self.db.collection(collName);
    if('function' == typeof(cond)) {
        callback = cond;
        cond = {};
        options = {};
    }
    if('function' == typeof(options)) {
      callback = options;
      options = {};
    }
    assert('string' == typeof field, 'field is not string');
    assert('function' == typeof callback, 'callback is not a function');
    MongoOP.distinct(collection, field, cond, options, function (err, arg) {
        callback(err, arg);
    });
}

function findUnion(collName, field, cond, options, callback) {
    assert('string' == typeof collName, 'collName is not string');
    assert('string' == typeof field, 'field is not string');

    let self = this;
    let collection = self.db.collection(collName);
    if('function' == typeof(cond)) {
        callback = cond;
        cond = {};
        options = { limit: 25 };
    }
    if('function' == typeof(options)) {
        callback = options;
        options = { limit: 25 };
    }
    let limit = options.limit || 25;
    let result = [];
    MongoOP.findCursor(collection, cond, options, function (err, cursor) {
        cursor.forEach(function(doc) {
            if(result.length >= limit) {
                cursor.close();
                return;
            }
            let unionid = doc[field];
            let index = _.findIndex(result, [field, unionid]);
            if(index == -1) {
                result.push(doc);
            }
        }, function() {
            callback(null, result);
        });
    });
}

Manager.prototype = {
    bindDb: _bindDb,

    _create: _create,   // args: id, name, json, callback
                        // ret: peterid
                        // for internal use
    create: create,     // args: name, json, callback
                        // ret: peterid
    createS: createS,   // args: name, json, callback
                        // ret: serial id.
                        // Create a peter with serial id. '@Schema.id' can be used as PeterId.
    destroy: destroy,   // args: peterid
    get: get,           // args: pid, fields (optional), options (optional), callback
                        // fields: can be an attribute name, or an array of attributes, or undefined/null
                        // ret: what specified in fields
    remove: remove,     // args: pid, fields, callback
                        // fields: can be an attribute name, or an array of attributes
                        // ret: none
    manyGet: manyGet,   // args: pids (array), fields (optional), callback
                        // fields: can be an attribute name, or an array of attributes, or undefined/null
                        // ret: what specified in fields
    find: find,         // args: collName, query, options, callback
                        // query and options can be an {} but not null
                        // default options is sort. you can also use fields, skip, limit, sort in options.
                        // ret: what specified in fields
    findOne: findOne,
    findUnion: findUnion,   // args: collName, field, cond, options, callback
    findOneAndUpdate: findOneAndUpdate,
    findOneAndReplace: findOneAndReplace,
    findOneAndDelete: findOneAndDelete,
    set: set,           // args: pid, json, [options], callback
                        // json: {key: value[, key: value]}
    replace: replace,   // args: pid, name, value, callback
                        // update the value of 'name' when it exists
    insert: insert,     // args: pid, name, value, callback
                        // insert <name:value> when it does not exist
    push: push,         // args: pid, container, element, [options, update: false], callback
                        // for list: just push
                        // for map: if the key exists, then push
                        // for set: if the value exists, then push
    pop: pop,           // args: pid, container, first (optional, default: false), callback
                        // ret: the element popped
    increase: increase, // args: pid, name, step, callback
    replaceElementByKey: replaceElementByKey, // args: pid, setname, element, replaceAll (optional), callback
    replaceElement: replaceElement,           // args: pid, setname, old_element, new_element, callback
    replaceElementByIndex: replaceElementByIndex, // args: pid, contname, index, _new, replaceAll (optional), callback
    removeElementByKey: removeElementByKey,   // args: pid, setname, key, callback
    removeElement: removeElement,             // args: pid, setname, elem, callback
    removeElementsByCond: removeElementsByCond, // args: pid, cont_name, cond, callback

    getElementByKey: getElementByKey,         // args: pid, setname, key, options (optional), callback
    getElementByIndex: getElementByIndex,     // TODO:
    getElementsByRange: getElementsByRange,   // args: pid, cont_name, range:[from, number], callback

    aggregate: aggregate,   // args: collName, cond, option, callback
    count: count,           // args: collName, cond, callback
    distinct: distinct,     // args: collName, field, cond, options, callback

    isExpectedPeter: isExpectedPeter,         // args: pid name; return boolean
    command: command4Monitor
};

module.exports = {
    createManager: function () {
        let mgr = new Manager();
        return Promise.promisifyAll(mgr);
    },

    version: VERSION
};
