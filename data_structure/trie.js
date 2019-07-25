/**
 * Created by linshiding on 1/3/16.
 */
let assert = require('assert');
let splitChar = require('../utils/split').splitChar;

function Trie(multiple) {
    this.root = [-1, {}];
    this.multi = multiple ? true : false;
}

Trie.prototype = {
    addWord: function (token, id, flag) {
        assert(token);
        let char = splitChar(token);
        let cur = this.root;
        for (let i=0; i<char.length; i++) {
            cur = (function (node, word) {
                if (word in node[1]) {
                    return node[1][word];
                }
                let _new = [-1, {}];
                node[1][word] = _new;
                return _new;
            })(cur, char[i]);
        }
        if (this.multi) {
            if (-1 == cur[0]) {
                cur[0] = {};
            }
            cur[0][id] = undefined==flag ? true : flag;
        } else {
            cur[0] = id;
        }
    },

    splitString: function (str) {
        let char = splitChar(str);
        if (char.length == 0) {
            return [];
        }
        let words = [];
        let cur = this.root;
        let from = 0, last = 0, save_pos = -1, save;
        for (let x=0; x<char.length; x++) {
            function _push(found, from, to) {
                for (let i=from; i<to; i++) {
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
            } else {
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
        } else if (last != char.length) {
            _push(-1, last, char.length);
        }
        return words;
    },

    find: function (str) {
        let char = splitChar(str);
        let cur = this.root;
        for (let x=0; x<char.length; x++) {
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
        for (let x in dict) {
            this.addWord(x, dict[x]);
        }
    }
};

module.exports = Trie;
