/**
 * Created by linshiding on 12/11/2016.
 */
var fs = require('fs');

fs.readdirSync(__dirname).forEach(function onfilename(filename) {
    if (!/\.js$/.test(filename) || /^_/.test(filename))
        return;

    var loc = __dirname+'/'+filename;
    var name = filename.substring(0, filename.length-3);
    var mod = require(loc);
    if (!mod)
        return;

    module.exports[name] = mod;
});
