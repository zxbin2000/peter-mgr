{{
    let util = require('util');

    function sortObj(obj) {
        let keys = Object.keys(obj).sort();
        let out = {};
        for (let x in keys) {
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
        let out = {};
        for (let x in objs) {
            let id = stringify(objs[x][attr]);
            if (!out[id]) {
                out[id] = [];
            }
            out[id].push(objs[x]);
        }
        return sortObj(out);
    }

    function aggrObjs(objs) {
        let out = {};
        for (let x in objs) {
            for (let y in objs[x]) {
                let id = stringify(y);
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
        let ret = aggrObjs($@);
        for (let y in ret) {
            console.log('\t%s: %d', y, ret[y].length);
        }
        $callback null 0
    }
    for (let x in attrs) {
        let ret = aggrAttrs($@, attrs[x]);
        console.log('Attr: %s', attrs[x]);
        for (let y in ret) {
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
