/**
 * Created by linshiding on 1/3/16.
 */
var assert = require('assert');
var splitChar = require('../utils/split').splitChar;

function Trie(multiple) {
    this.root = [-1, {}];
    this.multi = multiple ? true : false;
}

Trie.prototype = {
    addWord: function (token, id, flag) {
        assert(token);
        var char = splitChar(token);
        var cur = this.root;
        for (var i=0; i<char.length; i++) {
            cur = (function (node, word) {
                if (word in node[1]) {
                    return node[1][word];
                }
                var _new = [-1, {}];
                node[1][word] = _new;
                return _new;
            })(cur, char[i]);
        }
        if (this.multi) {
            if (-1 == cur[0]) {
                cur[0] = {};
            }
            cur[0][id] = undefined==flag ? true : flag;
        }
        else {
            cur[0] = id;
        }
    },

    splitString: function (str) {
        var char = splitChar(str);
        if (char.length == 0) {
            return [];
        }
        var words = [];
        var cur = this.root;
        var from = 0, last = 0, save_pos = -1, save;
        for (var x=0; x<char.length; x++) {
            function _push(found, from, to) {
                for (var i=from; i<to; i++) {
                    if (char[i] == ' ') {
                        if (from != i) {
                            words.push([found, char.slice(from, i).join('')]);
                        }
                        from = i+1;
                    }
                }
                if (from != to) {
                    words.push([found, char.slice(from, to).join('')]);
                }
            }

            //console.log('try', char.slice(from, x+1).join(''), char[x]);
            if (char[x] in cur[1]) {
                cur = cur[1][char[x]];
                if (-1 != cur[0]) {
                    //console.log('*');
                    save = cur[0];
                    save_pos = x+1;
                }
                if (x < char.length-1)
                    continue;
            }
            if (save) {
                //console.log('fallback', last, from, save_pos);
                if (from != last) {
                    _push(-1, last, from);
                }
                _push(save, from, save_pos);
                save = null;
                last = save_pos;
                from = save_pos;
                x = from - 1;
            }
            else {
                x = from;
                from ++;
            }
            cur = this.root;
        }
        if (-1 != cur[0]) {
            //console.log('found', cur[0]);
            assert(from!=x && -1!=from, from+':'+x+':'+str);
            if (from != last) {
                _push(-1, last, from);
            }
            _push(cur[0], from, x);
        }
        else if (last != char.length) {
            _push(-1, last, char.length);
        }
        return words;
    },

    find: function (str) {
        var char = splitChar(str);
        var cur = this.root;
        for (var x=0; x<char.length; x++) {
            if (char[x] in cur[1]) {
                cur = cur[1][char[x]];
                continue;
            }
            if (-1 != cur[0]) {
                return cur[0];
            }
        }
        return cur[0];
    },

    loadFromDict: function (dict) {
        for (var x in dict) {
            this.addWord(x, dict[x]);
        }
    }
};

module.exports = Trie;
//{
//    createTrie: function (multiple) {
//        var trie = new Trie(multiple);
//        return trie;
//    }
//};
