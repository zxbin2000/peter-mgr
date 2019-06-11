/**
 * Created by linshiding on 1/3/16.
 */
var assert = require('assert');

function splitChar(str) {
    var len = str.length;
    var from = 0;
    var out = [];
    var isblank = false;

    for (var i=0; i<len; i++) {
        function _push(separate) {
            if (from != i) {
                out.push(str.substring(from, i));
            }
            from = i + 1;
            if (separate) {
                if (!isblank) {
                    out.push(separate);
                }
                isblank = true;
            }
        }

        var code = str.charCodeAt(i);
        if (code <= 32 || code==65288 || code==65289) {               // skip space and unprintable
            _push(' ');
        }
        else if (code > 0x80) {
            _push();
            out.push(str.substring(i, i+1));
            isblank = false;
        }
        else {
            isblank = false;
        }
    }
    _push();

    if (out[0] == ' ') {
        out.splice(0, 1);
    }
    if (out[out.length-1] == ' ') {
        out.splice(out.length-1, 1);
    }
    return out;
}

module.exports = {
    splitChar: splitChar
};
