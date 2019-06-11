{{
    var util = require('util');

    function sortObj(obj) {
        var keys = Object.keys(obj).sort();
        var out = {};
        for (var x in keys) {
            out[keys[x]] = obj[keys[x]];
        }
        return out;
    }

    function stringify(obj) {
        if ('object' != typeof obj) {
            return util.inspect(obj);
        }
        if (obj instanceof Array) {
            return util.inspect(obj);
        }
        if (obj instanceof Date) {
            return '\'' + utils.timeString(obj) + '\'';
        }
        return util.inspect(sortObj(obj));
    }

    function aggrAttrs(objs, attr) {
        var out = {};
        for (var x in objs) {
            var id = stringify(objs[x][attr]);
            if (!out[id]) {
                out[id] = [];
            }
            out[id].push(objs[x]);
        }
        return sortObj(out);
    }

    function aggrObjs(objs) {
        var out = {};
        for (var x in objs) {
            for (var y in objs[x]) {
                var id = stringify(y);
                if (!out[id]) {
                    out[id] = [];
                }
                out[id].push(objs[x]);
            }
        }
        return sortObj(out);
    }
}}


::  ls collection attrs
=>  {{
    console.log('query', collection, attrs);
}}
    peter.query collection {} (attrs && attrs.length ? {project: attrs} : {})
<=  {{
    if (!attrs || !attrs.length) {
        var ret = aggrObjs($@);
        for (var y in ret) {
            console.log('\t%s: %d', y, ret[y].length);
        }
        $callback null 0
    }
    for (var x in attrs) {
        var ret = aggrAttrs($@, attrs[x]);
        console.log('Attr: %s', attrs[x]);
        for (var y in ret) {
            console.log('\t%s: %d', y, ret[y].length);
        }
    }
}}
    null 0
;;


::  main argv conf
=>  {{
    if (argv.length < 3) {
        console.log('Usage: ls_attr mongo collection [attr...]');
        $callback null 0
    }
}}
    peter.bindDb argv[1]
=>  ls argv[2] argv.slice(3)
;;

