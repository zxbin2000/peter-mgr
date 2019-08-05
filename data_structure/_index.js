/**
 * Created by linshiding on 12/11/2016.
 */
let fs = require('fs');

fs.readdirSync(__dirname).forEach(function onfilename(filename) {
    if (!/\.js$/.test(filename) || /^_/.test(filename))
        return;

    let loc = __dirname + '/' + filename;
    let name = filename.substring(0, filename.length - 3);
    let mod = require(loc);
    if (!mod)
        return;

    module.exports[name] = mod;
});
