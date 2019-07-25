let assert = require('assert');

function runEach(args, callback) {
    let obj = null;
    let func = args.shift();

    if ('function' != typeof func) {
        obj = func;
        func = args.shift();
    }
    args.push(callback);
    func.apply(obj, args);
}

/*
 ::  run what
 <=  ! what runEach $=
 ;;
 */
function run(what, callback) {
    assert(what && (what instanceof Array), 'Invalid parallel target');
    let n = Object.keys(what).length;
    let results = [];
    let ret = 0;

    for (let i=0; i<n; i++) {
        (function (x, y) {
            runEach(y, function (err, arg) {
                results[x] = [err, arg];
                assert(ret < n);
                ret ++;
                if (ret == n) {
                    process.nextTick(callback, null, results);
                }
            });
        })(i, what[i]);
    }
    if (0 == n) {
        process.nextTick(callback, null, results);
    }
}

module.exports = {
    run: run       // params: [ [cmd1, arg..], [cmd2, arg...], ... [cmdn, arg..] ]
};
