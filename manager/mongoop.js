/**
 * Created by linshiding on 3/10/15.
 */

let assert = require('assert');
let utils = require('../utils/utils');
let concurrent = require('../engine/concurrent');

let con = concurrent.create(200, 'Mongo Op');

function runMongoCmd(collection, cmd) {
    let args = Array.prototype.slice.call(arguments);
    let callback = args.pop();

    assert('function' === typeof callback, 'Invalid callback');
    
    let func = args[1];
    function callback_or_retry(err, arg) {
        if (null == err) {
            return callback(null, arg);
        }
        //console.log('Error:', err);
        let str = err.toString();
        if ('TypeError: Cannot read property \'maxBsonObjectSize\' of null' == str) {
            return setTimeout(function () {
                console.log('retry', str);
                args.unshift(func);
                args.unshift(collection);
                con.runArgs(args);
            }, 1000);
        } else if (-1 != str.search('"message":"write EPIPE"')
            || -1 != str.search('"message":"write EIO"')
            || -1 != str.search('"message":"read ECONNRESET"')
            || -1 != str.search('possible socket exception')
            || -1 != str.search('could not contact primary for')
            || -1 != str.search('ReplicaSetMonitor no master found')
            || -1 != str.search('not master')
            || -1 != str.search('write EADDRNOTAVAIL')
            || -1 != str.search('read ETIMEDOUT')
            || -1 != str.search('no mongos proxy available')
        ) {
            return setTimeout(function () {
                console.log('retry', str.substring(0, 512));
                args.unshift(func);
                args.unshift(collection);
                con.runArgs(args);
            }, 1000);
        }
        callback(err, arg);
    }

    args.push(callback_or_retry);
    con.runArgs(args);
}

function increase(collection, docid, name, step, callback) {
    assert(!utils.isNull(collection)
        && !utils.isNull(docid)
        && !utils.isNull(name)
        && utils.isNumber(step)
        && utils.isFunction(callback)
        , "Wrong parameters in query"
    );

    let inc = {};
    inc[name] = step;
    runMongoCmd(collection, collection.findOneAndUpdate,
        { _id: docid },
        { $inc: inc },
        { returnOriginal: false },
        (err, arg) => {
          callback(err, arg.ok ? arg.value : arg); 
        }
    );
}

function create(collection, json, callback) {
    assert(!utils.isNull(collection)
        && !utils.isNull(json)
        && utils.isFunction(callback)
        , "Wrong parameters in query"
    );
    
    runMongoCmd(collection, collection.insertOne, json, callback);
}

// fields can be an attribute name or an array of attributes
function get(collection, docid, fields, callback) {
    if(typeof fields === 'function') {
        callback = fields;
        fields = undefined;
    }
    assert(!utils.isNull(collection)
        && !utils.isNull(docid)
        && utils.isFunction(callback)
        , "Wrong parameters in query"
    );

    let query = {}
        , proj = {}
        , array = false;

    query['_id'] = docid;
    switch (typeof fields) {
        case 'undefined':
            array = true;
            break;
        case 'object':
            let t = utils.isArray(fields);
            for (let x in fields) {
                let k = t ? fields[x] : x;
                let v = t ? 1 : fields[x];
                proj[k] = v;
            }
            array = true;
            break;
        case 'string':
            proj[fields] = 1;
            break;
        default:
            assert(false, 'wrong type of fields ' + typeof fields);
            break;
    }
    let options = { projection: proj };

    runMongoCmd(collection, collection.findOne, query, options, function (err, arg) {
        if (null != err)
            return callback(err, arg);
        if (null == arg)
            return callback("Not existing", null);
        if (array)
            return callback(null, arg);

        return arg.hasOwnProperty(fields)
            ? callback(null, arg[fields])
            : callback("No such fields", null);
    });
}

function manyGet(collection, docids, fields, callback) {
    if(typeof fields === 'function') {
        callback = fields;
        fields = undefined;
    }
    assert(!utils.isNull(collection)
        && !utils.isNull(docids)
        && utils.isFunction(callback)
        , "Wrong parameters in query"
    );

    let query = {}
        , proj = {}
        , array = false;

    query['_id'] = { $in: docids };
    switch (typeof fields) {
    case 'undefined':
        array = true;
        break;

    case 'object':
        for (let x in fields) {
            proj[fields[x]] = 1;
        }
        array = true;
        break;

    case 'string':
        proj[fields] = 1;
        break;

    default:
        assert(false, "wrong type of fields");
        break;
    }

    let cursor = collection.find(query, { projection: proj });
    runMongoCmd(cursor, cursor.toArray, function (err, arg) {
        if (null != err)
            return callback(err, arg);
        if (null == arg)
            return callback("Not existing", null);

        let res = {};
        for (let x in arg) {
            if (array) {
                res[arg[x]["_id"]] = arg[x];
            }
            else {
                res[arg[x]["_id"]] = arg[x].hasOwnProperty(fields)
                    ? arg[x][fields] : null;
            }
        }
        callback(null, res);
    });
}

function set(collection, docid, json, options, callback) {
    if(typeof options === 'function') {
        callback = options;
        options = {};
    }
    assert(!utils.isNull(collection)
        && !utils.isNull(docid)
        && utils.isFunction(callback)
        , "Wrong parameters in query"
    );

    let cond;
    if (options.cond) {
        cond = options.cond;
        cond._id = docid;
        delete options.cond;
    } else {
        cond = {_id: docid};
    }
    runMongoCmd(collection, collection.updateOne, cond, {$set: json}, options, function (err, arg) {
        if (null != err)
            return callback(err, arg);
        if (0 == arg.result.n)
            return callback("Not existing", null);
        callback(null, arg.result);
    });
}

function replace(collection, docid, name, json, options, callback) {
    if(typeof options === 'function') {
        callback = options;
        options = {};
    }
    assert(!utils.isNull(collection)
        && !utils.isNull(docid)
        && !utils.isNull(name)
        && utils.isFunction(callback)
        , "Wrong parameters in query"
    );

    let cond = {_id: docid};
    cond[name] = { $exists: true };

    runMongoCmd(collection, collection.updateOne, cond, {$set: json}, options, function (err, arg) {
        if (null != err)
            return callback(err, arg);
        if (0 == arg.result.nModified)
            return callback("Not existing", null);
        callback(null, arg);
    });
}

function insert(collection, docid, name, json, options, callback) {
    if(typeof options === 'function') {
        callback = options;
        options = {};
    }
    assert(!utils.isNull(collection)
        && !utils.isNull(docid)
        && !utils.isNull(name)
        && utils.isFunction(callback)
        , "Wrong parameters in query"
    );

    let cond = { _id: docid };
    cond[name] = { $exists: false };
    runMongoCmd(collection, collection.updateOne, cond, { $set: json }, options, function (err, arg) {
        if (null != err)
            return callback(err, arg);
        if (0 == arg.result.nModified)
            return callback("Already existed", null);
        callback(null, arg);
    });
}

function remove(collection, docid, fields, callback) {
    assert(!utils.isNull(collection)
        && !utils.isNull(docid)
        && !utils.isNull(fields)
        && utils.isFunction(callback)
        , "Wrong parameters in query"
    );

    let cond = { _id: docid };
    let unset = {};
    switch (typeof fields) {
      case 'object':
          for (let x in fields) {
              unset[fields[x]] = 1;
          }
          break;

      case 'string':
          unset[fields] = 1;
          break;

      default:
          assert(false, "wrong type of fields");
          break;
    }

    runMongoCmd(collection, collection.updateOne, cond, { $unset: unset }, function (err, arg) {
        if (null != err)
            return callback(err, arg);
        if (0 == arg.result.nModified)
            return callback("Not existing", null);
        callback(null, arg.result);
    });
}

function pushList(collection, docid, listname, elem, callback) {
    assert(!utils.isNull(collection)
        && !utils.isNull(docid)
        && !utils.isNull(listname)
        && utils.isFunction(callback)
        , "Wrong parameters in query"
    );

    let cond = {}, add = {};
    cond['_id'] = docid;
    add[listname] = elem;

    runMongoCmd(collection, collection.updateOne, cond, { $push: add }, function (err, arg) {
        if (null != err)
            return callback(err, arg);
        callback(null, arg);
    });
}

function pushMap(collection, docid, setname, keyname, elem, callback) {
    assert(!utils.isNull(collection)
        && !utils.isNull(docid)
        && !utils.isNull(setname)
        && !utils.isNull(keyname)
        && utils.isFunction(callback)
        , "Wrong parameters in query"
    );
    assert(elem.hasOwnProperty(keyname), `elem han't: ${keyname}.`);

    let setwithkey = setname + '.' + keyname
        , cond = {}
        , add = {};

    cond[setwithkey] = { $ne: elem[keyname] };
    cond['_id'] = docid;
    add[setname] = elem;

    runMongoCmd(collection, collection.updateOne, cond, { $push: add }, { returnOriginal: false }, function (err, arg) {
        if (null === err && (arg.result && arg.result.nModified) === 0) {
            return callback("Already existed", null);
        }

        return callback(null, arg.result ? arg.result : arg);
    });
}

function pushSet(collection, docid, setname, elem, callback) {
    assert(!utils.isNull(collection)
        && !utils.isNull(docid)
        && !utils.isNull(setname)
        && utils.isFunction(callback)
        , "Wrong parameters in query"
    );

    let cond = {}, add = {};
    cond['_id'] = docid;
    add[setname] = elem;

    runMongoCmd(collection, collection.updateOne, cond, { $addToSet: add }, callback);
}

function pop(collection, docid, setname, first, callback) {
    assert(!utils.isNull(collection)
        && !utils.isNull(docid)
        && !utils.isNull(setname)
        && utils.isFunction(callback)
        , "Wrong parameters in query"
    );

    let cond = {}
        , update = {}
        , fields = {};

    cond['_id'] = docid;
    cond[setname] = { $exists: true };
    update[setname] = first ? -1 : 1;
    fields[setname] = 1;
    
    runMongoCmd(collection, collection.findOneAndUpdate, cond, { $pop: update }, { 
        projection: fields,
        returnOriginal: false
    }, function (err, arg) {
        if (null != err)
            return callback(err, arg);

        let ret = arg.value ? arg.value[setname] : undefined;
        if (undefined == ret || ret.length < 1)
            return callback("Not existing", null);

        callback(null, ret);
    });
}

//replaceAll means to replace the whole element, otherwise only replace the attributes specified in $elem
function replaceMap(collection, docid, setname, keyname, elem, replaceAll, callback) {
    assert(!utils.isNull(collection)
        && !utils.isNull(docid)
        && !utils.isNull(setname)
        && !utils.isNull(keyname)
        && utils.isFunction(callback)
        , "Wrong parameters in query"
    );
    assert(elem.hasOwnProperty(keyname), `elem han't: ${keyname}.`);

    let setwithkey = setname + '.' + keyname
        , cond = {}
        , update = {};

    cond[setwithkey] = elem[keyname];
    cond['_id'] = docid;
    if (replaceAll) {
        update[setname + '.$'] = elem;
    } else {
        for (let x in elem) {
            if (x != keyname)
                update[setname + '.$.' + x] = elem[x];
        }
    }

    runMongoCmd(collection, collection.updateOne, cond, { $set: update }, function (err, arg) {
        if (null != err)
            return callback(err, arg);
        if (0 == arg)
            return callback("Not existing", null);
        callback(null, arg);
    });
}

function replaceSet(collection, docid, setname, old, _new, callback) {
    assert(!utils.isNull(collection)
        && !utils.isNull(docid)
        && !utils.isNull(setname)
        && utils.isFunction(callback)
        , "Wrong parameters in query"
    );

    let cond = {}
        , update = {};

    cond['_id'] = docid;
    cond[setname] = { $in: [old], $ne: _new };
    update[setname + '.$'] = _new;

    runMongoCmd(collection, collection.updateOne, cond, { $set: update }, function (err, arg) {
        if (null != err)
            return callback(err, arg);
        if (0 == arg)
            return callback("Not existing or duplicated", null);
        callback(null, arg);
    });
}

function removeSet(collection, docid, setname, value, callback) {
    assert(!utils.isNull(collection)
        && !utils.isNull(docid)
        && !utils.isNull(setname)
        && utils.isFunction(callback)
        , "Wrong parameters in query"
    );

    let cond = {}
        , update = {};

    cond['_id'] = docid;
    cond[setname] = value;
    update[setname] = value;

    runMongoCmd(collection, collection.updateOne, cond, { $pull: update }, function (err, arg) {
        if (null != err)
            return callback(err, arg);
        if (0 == arg.result.nModified)
            return callback("Not existing", null);
        callback(null, arg);
    });
}

//replaceAll means to replace the whole element, otherwise only replace the attributes specified in $elem
function removeMap(collection, docid, setname, keyname, keyvalue, callback) {
    assert(!utils.isNull(collection)
        && !utils.isNull(docid)
        && !utils.isNull(setname)
        && !utils.isNull(keyname)
        && utils.isFunction(callback)
        , "Wrong parameters in query"
    );

    let setwithkey = setname + '.' + keyname
        , cond = {}
        , update = {}
        , key = {};

    cond[setwithkey] = keyvalue;
    cond['_id'] = docid;
    key[keyname] = keyvalue;
    update[setname] = key;

    runMongoCmd(collection, collection.updateOne, cond, { $pull: update }, function (err, arg) {
        if (null != err)
            return callback(err, arg);
        if (0 == arg.result.nModified)
            return callback("Not existing", null);
        callback(null, arg);
    });
}

//replaceAll means to replace the whole element, otherwise only replace the attributes specified in $elem
function replaceByIndex(collection, docid, cont, index, elem, replaceAll, callback) {
    assert(!utils.isNull(collection)
        && !utils.isNull(docid)
        && !utils.isNull(cont)
        && utils.isFunction(callback)
        , "Wrong parameters in query"
    );

    let cond = {}
      , update = {};

    cond['_id'] = docid;
    let setname = cont + '.' + index;
    if (replaceAll) {
        update[setname] = elem;
    } else {
        for (let x in elem) {
            if (x != keyname)
                update[setname + '.' + x] = elem[x];
        }
    }

    runMongoCmd(collection, collection.updateOne, cond, { $set: update }, function (err, arg) {
        if (null != err)
            return callback(err, arg);
        if (0 == arg)
            return callback("Not existing", null);
        callback(null, arg);
    });
}

function getElementsByCond(collection, docid, listname, cond, callback) {
    assert(!utils.isNull(collection)
        && !utils.isNull(docid)
        && !utils.isNull(listname)
        && !utils.isNull(cond)
        && utils.isFunction(callback)
        , "Wrong parameters in query"
    );

    let query = {}
        , proj = {};

    query['_id'] = docid;
    proj[listname] = {$elemMatch: cond};

    runMongoCmd(collection, collection.findOne, query, proj, function (err, arg) {
        if (null != err)
            return callback(err, arg);
        if (null == arg || !arg.hasOwnProperty(listname) || arg[listname].length < 1)
            return callback("Not existing", null);

        callback(null, arg[listname]);
    });
}

// cond: {name : value}
function removeElementsByCond(collection, docid, listname, cond, callback) {
    assert(!utils.isNull(collection)
        && !utils.isNull(docid)
        && !utils.isNull(listname)
        && !utils.isNull(cond)
        && utils.isFunction(callback)
        , "Wrong parameters in query"
    );

    let query = {}
        , update = {};

    query['_id'] = docid;
    update[listname] = cond;

    runMongoCmd(collection, collection.updateOne, query, {$pull: update}, function (err, arg) {
        if (null != err)
            return callback(err, arg);
        if (0 == arg.result.n)
            return callback("Not existing", null);
        if (0 == arg.result.nModified)
            return callback("Not such field", null);
        callback(null, arg);
    });
}

function getElementsByRange(collection, docid, listname, range, callback) {
    assert(!utils.isNull(collection)
        && !utils.isNull(docid)
        && !utils.isNull(listname)
        && utils.isFunction(callback)
        , "Wrong parameters in query"
    );

    let query = {}
        , proj = {};

    query['_id'] = docid;
    proj[listname] = {$slice: range};

    runMongoCmd(collection, collection.findOne, query, proj, function (err, arg) {
        if (null != err)
            return callback(err, arg);
        if (null == arg || !arg.hasOwnProperty(listname) || arg[listname].length < 1)
            return callback("Not existing", null);

        callback(null, arg[listname]);
    });
}


function find(collection, query, options, callback) {
    if('function' == typeof options) {
        callback = options;
        options = {};
    }
    assert(!utils.isNull(collection)
        && !utils.isNull(query)
        && (!utils.isNull(options) || utils.isFunction(callback))
        , "Wrong parameters in query"
    );

    let cursor = collection.find(query, options);
    runMongoCmd(cursor, cursor.toArray, callback);
}

function findOne(collection, filter, options, callback) {
    assert(!utils.isNull(collection)
        && !utils.isNull(filter)
        && (!utils.isNull(options) || utils.isFunction(callback))
        , "Wrong parameters in query"
    );

    runMongoCmd(collection, collection.findOne, filter, options, callback);
}

function findOneAndUpdate(collection, filter, update, options, callback) {
    assert(!utils.isNull(collection) 
        && !utils.isNull(filter) 
        && (!utils.isNull(update) || utils.isFunction(callback)) 
        , "Wrong parameters in query"
    );

    runMongoCmd(collection, collection.findOneAndUpdate, filter, update, options, function (err, arg) {
        callback(err, null == err ? arg.value : arg);
    });
}

function findOneAndDelete(collection, filter, options, callback) {
    assert(!utils.isNull(collection) 
        && !utils.isNull(filter)
        && (!utils.isNull(options) || utils.isFunction(callback)) 
        , "Wrong parameters in query"
    );

    runMongoCmd(collection, collection.findOneAndDelete, filter, options, function (err, arg) {
        callback(err, null == err ? arg.value : arg);
    });
}

function findOneAndReplace(collection, filter, replacement, options, callback) {
    assert(!utils.isNull(collection) 
        && !utils.isNull(filter)
        && !utils.isNull(replacement)
        && (!utils.isNull(options) || utils.isFunction(callback)) 
        , "Wrong parameters in query"
    );

    runMongoCmd(collection, collection.findOneAndReplace, filter, replacement, options, function (err, arg) {
        callback(err, null == err ? arg.value : arg);
    });
}

function destroy(collection, filter, options, callback) {
    assert(!utils.isNull(collection) 
        && !utils.isNull(filter)
        && (!utils.isNull(options) || utils.isFunction(callback)) 
        , "Wrong parameters in query"
    );

    runMongoCmd(collection, collection.deleteOne, filter, options, callback);
}

function aggregate(collection, cond, options, callback) {
    assert(!utils.isNull(collection) 
        && !utils.isNull(cond)
        && (!utils.isNull(options) || utils.isFunction(callback)) 
        , "Wrong parameters in query"
    );

    switch (typeof(options)) {
        case 'function':
            callback = options;
            options = {};
            break;
        case 'object':
            options = options;
            break;
        default :
            options = {};
    }
    let cursor = collection.aggregate(cond, options);
    runMongoCmd(cursor, cursor.toArray, callback);
}

function count(collection, cond, callback) {
    assert(!utils.isNull(collection) 
        && !utils.isNull(cond)
        && utils.isFunction(callback)
        , "Wrong parameters in query"
    );

    runMongoCmd(collection, collection.countDocuments, cond, callback);
}

function distinct(collection, field, cond, options, callback) {
    assert(!utils.isNull(collection) 
        && !utils.isNull(field)
        && !utils.isNull(cond)
        && (!utils.isNull(options) || utils.isFunction(callback)) 
        , "Wrong parameters in distinct"
    );

    runMongoCmd(collection, collection.distinct, field, cond, options, function (err, arg) {
        callback(err, arg);
    });
}

function findCursor(collection, cond, options, callback) {
    assert(!utils.isNull(collection)
        && !utils.isNull(cond)
        && (!utils.isNull(options) || utils.isFunction(callback))
        , "Wrong parameters in findCursor"
    );

    callback(null, collection.find(cond, options));
}

module.exports = {
    create: create,
    destroy: destroy,
    increase: increase,
    get: get,
    manyGet: manyGet,
    find: find,
    findOne: findOne,
    findOneAndUpdate: findOneAndUpdate,
    findOneAndDelete: findOneAndDelete,
    findOneAndReplace: findOneAndReplace,
    set: set,
    replace: replace,
    insert: insert,
    remove: remove,
    pushList: pushList,
    pushSet: pushSet,
    pushMap: pushMap,
    pop: pop,
    replaceSet: replaceSet,
    replaceMap: replaceMap,
    removeSet: removeSet,
    removeMap: removeMap,
    removeElementsByCond: removeElementsByCond,

    replaceElementByIndex: replaceByIndex,

    getElementsByCond: getElementsByCond,
    getElementsByRange: getElementsByRange,

    aggregate: aggregate,
    count: count,
    distinct: distinct,
    findCursor: findCursor
};