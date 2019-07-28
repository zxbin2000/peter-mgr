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
    assert('function' == typeof callback);
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
        }
        else if (-1 != str.search('"message":"write EPIPE"')
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

function add(collection, docid, name, value, callback) {
    assert(callback);
    let inc = {};
    inc[name] = value;

    runMongoCmd(collection, collection.findOneAndUpdate,
        {_id: docid},
        {$inc: inc},
        {upsert: true, returnNewDocument: true, returnOriginal: false},
        function (err, arg) {
            callback(err, null == err ? arg.value : arg);
        });
}

function create(collection, json, callback) {
    assert(callback);
    
    runMongoCmd(collection, collection.insertOne, json, callback);
}

// fields can be an attribute name or an array of attributes
function get(collection, docid, fields, callback) {
    assert(callback);
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
    assert(callback);
    let query = {}
        , proj = {}
        , array = false;

    query['_id'] = {$in: docids};
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

    let cursor = collection.find(query, proj);
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

function getMany(collection, docids, fields, callback) {
    assert(callback);
    let query = {}
        , proj = {}
        , array = false;

    let map = {};
    for (let x in docids) {
        let id = docids[x];
        if (map[id]) {
            map[id].push(+x);
        }
        else {
            map[id] = [+x];
        }
    }
    query['_id'] = {$in: docids};
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

    let cursor = collection.find(query, proj);
    runMongoCmd(cursor, cursor.toArray, function (err, arg) {
        if (null != err)
            return callback(err, arg);
        if (null == arg)
            return callback("Not existing", null);

        let res = [];
        for (let x in arg) {
            let obj = array
                    ? arg[x]
                    : (arg[x].hasOwnProperty(fields) ? arg[x][fields] : null);

            let ords = map[arg[x]._id];
            for (let i in ords) {
                res[ords[i]] = obj;
            }
        }
        callback(null, res);
    });
}

function set(collection, docid, json, options, callback) {
    assert(callback);
    let cond;
    if (options.cond) {
        cond = options.cond;
        cond._id = docid;
        delete options.cond;
    }
    else {
        cond = {_id: docid};
    }
    runMongoCmd(collection, collection.updateOne, cond, {$set: json}, options, function (err, arg) {
        if (null != err)
            return callback(err, arg);
        if (0 == arg.result.n)
            return callback("Not existing", null);
        callback(null, arg);
    });
}

function replace(collection, docid, name, json, options, callback) {
    assert(callback);
    let cond = {_id: docid};

    cond[name] = {$exists: true};
    runMongoCmd(collection, collection.updateOne, cond, {$set: json}, options, function (err, arg) {
        if (null != err)
            return callback(err, arg);
        if (0 == arg.result.nModified)
            return callback("Not existing", null);
        callback(null, arg);
    });
}

function insert(collection, docid, name, json, options, callback) {
    assert(callback);
    let cond = { _id: docid };

    cond[name] = {$exists: false};
    runMongoCmd(collection, collection.updateOne, cond, {$set: json}, options, function (err, arg) {
        if (null != err)
            return callback(err, arg);
        if (0 == arg.result.nModified)
            return callback("Already existed", null);
        callback(null, arg);
    });
}

function remove(collection, docid, fields, callback) {
    assert(fields);
    assert(callback);
    let cond = {_id: docid};
    let unset = {};
    switch (typeof fields) {
    case 'object':
        for (let x in fields) {
            unset[fields[x]] = 1;
        }
        break;

    case 'string':
        unset[fields] = '';
        break;

    default:
        assert(false, "wrong type of fields");
        break;
    }

    runMongoCmd(collection, collection.updateOne, cond, {$unset: unset}, function (err, arg) {
        if (null != err)
            return callback(err, arg);
        if (0 == arg.result.nModified)
            return callback("Not existing", null);
        callback(null, arg);
    });
}

function pushList(collection, docid, listname, elem, upsert, callback) {
    assert(callback);
    let cond = {}
        , add = {};

    cond['_id'] = docid;
    add[listname] = elem;

    runMongoCmd(collection, collection.updateOne, cond, {$push: add}, {upsert: upsert}, function (err, arg) {
        if (null != err)
            return callback(err, arg);
        if (0 == arg.result.n)
            return callback("Not existing", null);
        callback(null, arg);
    });
}

function pushMap(collection, docid, setname, keyname, elem, upsert, callback) {
    assert(callback);
    assert(elem.hasOwnProperty(keyname), 'elem.hasOwnProperty(keyname)');
    let setwithkey = setname + '.' + keyname
        , cond = {}
        , add = {};

    cond[setwithkey] = {$ne: elem[keyname]};
    cond['_id'] = docid;
    add[setname] = elem;

    runMongoCmd(collection, collection.updateOne, cond, {$push: add}, {upsert: upsert}, function (err, arg) {
        if (null != err) {
            return callback(11000 == err.code ? "Already existed" : err, 0);
        }
        if (1 != arg.result.n || 1 != arg.result.ok) {
            return callback("Not existing", 0);
        }
        return callback(null, arg);
    });
}

function pushSet(collection, docid, setname, elem, upsert, callback) {
    assert(callback);
    let cond = {}
        , add = {};

    cond[setname] = {$ne: elem};
    cond['_id'] = docid;
    add[setname] = elem;

    runMongoCmd(collection, collection.updateOne, cond, {$push: add}, {upsert: upsert}, function (err, arg) {
        if (null != err) {
            return callback(11000 == err.code ? "Already existed" : err, 0);
        }
        if (1 != arg.result.n || 1 != arg.result.ok) {
            return callback("Not existing", 0);
        }
        return callback(null, arg);
    });
}

// first is default to false
function pop(collection, docid, setname, first, callback) {
    assert(callback);

    let cond = {}
        , update = {}
        , fields = {};

    cond['_id'] = docid;
    cond[setname] = {$exists: true};
    update[setname] = first ? -1 : 1;
    fields[setname] = 1;//{ '$slice': 1 };
    // !! bug in mongodb driver, can't accept $slice modifier
    runMongoCmd(collection, collection.findOneAndUpdate, cond, [], {$pop: update}, {fields: fields}, function (err, arg) {
        if (null != err)
            return callback(err, arg);

        let ret = arg[setname];
        if (undefined == ret || ret.length < 1)
            return callback("Not existing", null);

        // !! bug in mongodb driver, can't accept $slice modifier, so we have to do it ourselves
        callback(null, first ? ret.shift() : ret.pop());
    });
}

//replaceAll means to replace the whole element, otherwise only replace the attributes specified in $elem
function replaceMap(collection, docid, setname, keyname, elem, replaceAll, callback) {
    assert(callback);
    assert(elem.hasOwnProperty(keyname), 'elem.hasOwnProperty(keyname)');

    let setwithkey = setname + '.' + keyname
        , cond = {}
        , update = {};

    cond[setwithkey] = elem[keyname];
    cond['_id'] = docid;
    if (replaceAll) {
        update[setname + '.$'] = elem;
    }
    else {
        for (let x in elem) {
            if (x != keyname)
                update[setname + '.$.' + x] = elem[x];
        }
    }

    runMongoCmd(collection, collection.updateOne, cond, {$set: update}, function (err, arg) {
        if (null != err)
            return callback(err, arg);
        if (0 == arg)
            return callback("Not existing", null);
        callback(null, arg);
    });
}

function replaceSet(collection, docid, setname, old, _new, callback) {
    assert(callback);
    let cond = {}
        , update = {};

    cond['_id'] = docid;
    cond[setname] = {$in: [old], $ne: _new};
    update[setname + '.$'] = _new;

    runMongoCmd(collection, collection.updateOne, cond, {$set: update}, function (err, arg) {
        if (null != err)
            return callback(err, arg);
        if (0 == arg)
            return callback("Not existing or duplicated", null);
        callback(null, arg);
    });
}

function removeSet(collection, docid, setname, value, callback) {
    assert(callback);
    let cond = {}
        , update = {};

    cond['_id'] = docid;
    cond[setname] = value;
    update[setname] = value;

    runMongoCmd(collection, collection.updateOne, cond, {$pull: update}, function (err, arg) {
        if (null != err)
            return callback(err, arg);
        if (0 == arg.result.nModified)
            return callback("Not existing", null);
        callback(null, arg);
    });
}

//replaceAll means to replace the whole element, otherwise only replace the attributes specified in $elem
function removeMap(collection, docid, setname, keyname, keyvalue, callback) {
    assert(callback);
    let setwithkey = setname + '.' + keyname
        , cond = {}
        , update = {}
        , key = {};

    cond[setwithkey] = keyvalue;
    cond['_id'] = docid;
    key[keyname] = keyvalue;
    update[setname] = key;

    runMongoCmd(collection, collection.updateOne, cond, {$pull: update}, function (err, arg) {
        if (null != err)
            return callback(err, arg);
        if (0 == arg.result.nModified)
            return callback("Not existing", null);
        callback(null, arg);
    });
}

//replaceAll means to replace the whole element, otherwise only replace the attributes specified in $elem
function replaceByIndex(collection, docid, cont, index, elem, replaceAll, callback) {
    assert(callback);
    let cond = {}
      , update = {};

    cond['_id'] = docid;
    let setname = cont + '.' + index;
    if (replaceAll) {
        update[setname] = elem;
    }
    else {
        for (let x in elem) {
            if (x != keyname)
                update[setname + '.' + x] = elem[x];
        }
    }

    runMongoCmd(collection, collection.updateOne, cond, {$set: update}, function (err, arg) {
        if (null != err)
            return callback(err, arg);
        if (0 == arg)
            return callback("Not existing", null);
        callback(null, arg);
    });
}

// cond: {name : value}
function getElementsByCond(collection, docid, listname, cond, callback) {
    assert(callback);
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
    assert(callback);
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


// range: [from, number]
function getElementsByRange(collection, docid, listname, range, callback) {
    assert(callback);
    assert(range instanceof Array);

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
        && (!utils.isNull(options) || !utils.isNull(callback))
        , "Wrong parameters in query"
    );

    let cursor = collection.find(query, options);
    runMongoCmd(cursor, cursor.toArray, callback);
}

function findOne(collection, filter, options, callback) {
    assert(!utils.isNull(collection)
        && !utils.isNull(filter)
        && (!utils.isNull(options) || !utils.isNull(callback))
        , "Wrong parameters in query"
    );
    runMongoCmd(collection, collection.findOne, filter, options, callback);
}

function findOneAndUpdate(collection, filter, update, options, callback) {
    assert(!utils.isNull(collection) 
        && !utils.isNull(filter) 
        && (!utils.isNull(update) || !utils.isNull(callback)) 
        , "Wrong parameters in query"
    );
    runMongoCmd(collection, collection.findOneAndUpdate, filter, update, options, function (err, arg) {
        callback(err, null == err ? arg.value : arg);
    });
}

function findOneAndDelete(collection, filter, options, callback) {
    assert(!utils.isNull(collection) 
        && !utils.isNull(filter)
        && (!utils.isNull(options) || !utils.isNull(callback)) 
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
        && (!utils.isNull(options) || !utils.isNull(callback)) 
        , "Wrong parameters in query"
    );
    runMongoCmd(collection, collection.findOneAndReplace, filter, replacement, options, function (err, arg) {
        callback(err, null == err ? arg.value : arg);
    });
}

function destroy(collection, filter, options, callback) {
    assert('function' == typeof(callback), 'Invalid callback');

    runMongoCmd(collection, collection.deleteOne, filter, options, callback);
}

// cond: [{stage}, {stage}]
function aggregate(collection, cond, options, callback) {
    //assert(!utils.isNull(collection)
    //    && !utils.isNull(cond),
    //    "Wrong parameters in aggregate"
    //);
    assert(callback);

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

// cond: { query condition }
function count(collection, cond, callback) {
    assert(callback);
    runMongoCmd(collection, collection.countDocuments, cond, callback);
}

function distinct(collection, field, cond, options, callback) {
    assert(!utils.isNull(collection) 
        && !utils.isNull(field)
        && !utils.isNull(cond)
        && (!utils.isNull(options) || !utils.isNull(callback)) 
        , "Wrong parameters in distinct"
    );
    runMongoCmd(collection, collection.distinct, field, cond, options, function (err, arg) {
        callback(err, arg);
    });
}

function findCursor(collection, cond, options, callback) {
    assert(!utils.isNull(collection)
        && !utils.isNull(cond)
        && (!utils.isNull(options) || !utils.isNull(callback))
        , "Wrong parameters in findCursor"
    );
    let cursor = collection.find(cond, options);
    callback(null, cursor);
}

module.exports = {
    create: create,
    destroy: destroy,
    add: add,
    get: get,
    manyGet: manyGet,
    getMany: getMany,
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