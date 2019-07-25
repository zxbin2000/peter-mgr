let utils = require('../utils/utils');
let Path = require('path');
let assert = require('assert');
let fs = require('fs');
let sprintf = require("sprintf-js").sprintf;
let ascii = require('../utils/ascii');
let beautify = require('js-beautify').js_beautify;
let os = require('os');
let Parser = require('../manager/parser');
let jsonic = require('jsonic');

let dollarCode = ascii['$'];
let digitCode0 = ascii['0'];
let digitCode9 = ascii['9'];
let dashCode = ascii['-'];
let g_parsed;

function procArg(rule, param) {
    assert(null != rule);
    if (0 == param.length) {
        return false;
    }

    let notnull = false;
    if ('NOTNULL' == param[0]) {
        notnull = true;
        param.shift();

        if (0 == param.length) {
            return false;
        }
        rule.notnull = [];
    }

    for (let x in param) {
        if (notnull) {
            rule.notnull.push(param[x]);
        }
    }
    return true;
}

function resolvePath(file) {
    return Path.resolve(Path.dirname(g_parsed.path), file);
}

function translateResolve(param, what) {
    let level = 0;
    let done = false;
    let i;
    for (i=what; i<param.length; i++) {
        if ('(' == param[i]) {
            if (0 == level) {
                param[i] = 'Path.resolve(';
            }
            level ++;
        }
        else if (')' == param[i]) {
            level --;
            if (0 == level) {
                break;
            }
        }
        else if ('$$$' == param[i]) {
            if (!done) {
                param[i] = g_parsed.path ? ('\'' + Path.dirname(g_parsed.path) + '\',') : '\'.\',';
                done = true;
            }
            else {
                param.splice(i, 1);
                i--;
            }
        }
    }
    if (0!=level || i==param.length || i==what || !done) {
        throw new Error('Wrong arguments for $resolve: '+param.slice(what, param.length-what).join(''));
    }
    let str = param.splice(what+1, i-what+1).join('');
    return str;
}

let g_TranslateTable = {
    '$=>': '$return', '$return': '$return', '$<=': '$callback', '$callback': '$callback',
    '$@': '__arg', '$?': '__err', '$_': '__x', '$=': '__y', '$-': '__ind',
    '$^': '__http_headers', '$%': '__http_cookies', '$&': '__http_files', '$*': '__http_request', '$>': '__http_response',
    '$dir': function () {
        return g_parsed.path ? ('\'' + Path.dirname(g_parsed.path) + '\'') : 'undefined';
    },
    '$file': function () {
        return '\'' + g_parsed.path + '\'';
    },
    '$app': function () {
        return '\'' + Path.basename(g_parsed.path, '.pp') + '\'';
    },

    '$resolve': translateResolve,

    '$include': function (param, what) {
        assert(param.length >= what+2, 'Wrong parameters for $include');
        let str = param.splice(what+1, param.length-what).join('');
        let filename = eval(str);
        assert(filename, 'Wrong filename for $include');
        let filePath = resolvePath(filename);
        let fileExists = fs.existsSync(filePath);
        if (!fileExists) {
            let errorMessage = '$include file : ' + filePath + ' is not exists';
            throw new Error(errorMessage);
        }
        _includeFileCode(filePath);
        return '';
    }
};
function _includeFileCode(filePath) {
    let isDirector = fs.statSync(filePath).isDirectory();
    if (isDirector) {
        let fileArray = fs.readdirSync(filePath);
        fileArray.forEach(function (subFileName) {
            let subFilePath = filePath + '/' + subFileName;
            let ifFile = fs.statSync(subFilePath).isFile();
            if (ifFile) {
                let suffix = Path.extname(subFilePath);
                if (['.js', '.pp'].indexOf(suffix) >= 0) {
                    _includeFileCode(subFilePath);
                }
            } else {
                _includeFileCode(subFilePath);
            }

        });
    } else {
        let suffix = Path.extname(filePath);
        let code = fs.readFileSync(filePath).toString();
        switch (suffix) {
            case '.js':
                appendGlobal(['{{']);
                appendGlobal(code.split('\n'));
                appendGlobal(['}}']);
                break;
            case '.pp':
                appendGlobal(code.split('\n'));
                break;
            default:
                throw(new Error('Unknown suffix: '+suffix));
                break;
        }
    }
}
function translateParameter(global, cond, param, what) {
    let ball = param[what];
    assert(ball.charCodeAt(0) == dollarCode);
    let len;
    for (let x in g_TranslateTable) {
        len = x.length;
        if (ball.substring(0, len) == x) {
            let to = g_TranslateTable[x];
            if ('function' == typeof to) {
                return to(param, what) + ball.substring(len);
            }
            return to + ball.substring(len);
        }
    }
    let str = false;
    let from = 1;
    if (ball.charCodeAt(1) == dollarCode) {
        str = true;
        from = 2;
    }
    let n = parseInt(ball.substring(from));
    if (isNaN(n)) {
        //return '$$TODO';
        return ball;
    }
    if (null == global)
        return null;
    if (n > global.target.length || n < 0) {
        console.error("Error: wrong parameter %s", ball);
        return null;
    }
    // $n
    return str ? ('\'' + global.proto[n].name + '\'') : global.proto[n].name;
}

function checkFuncString(global, cond, param, isFunc) {
    assert(0 < param.length, '0 < param.length');
    let level = 0
      , first = -2
      , str;

    //console.log(1, param);
    for (let x = 0; x < param.length; x++) {
        if ('$' == param[x].substring(0, 1) && '$$$' != param[x]) {
            param[x] = translateParameter(global, cond, param, x);
            if (null == param[x]) {
                return false;
            }
        }
    }
    for (let x = 0; x < param.length; x++) {
        //console.log("check %d %d %s, first=%d, %s", level, x, param[x], first, JSON.stringify(param.slice(0, x)));
        switch (param[x].substring(0, 1)) {
        case '(':
        case '{':
        case '[':
        //case '<':
            if (0 == level++) {
                first = x;
            }
            if (x > 0 && x < param.length - 1 && param[x + 1] == '$$$') {
                str = param[x - 1] + param[x];
                param.splice(x - 1, 3, str);
                x--;
                if (first > x) {
                    first = x;
                }
            }
            break;

        case ')':
        case '}':
        case ']':
        //case '>':
            if (--level == 0) {
                str = '';
                for (let i = first; i <= x; i++) {
                    //console.log("%d: %s, %s", i, param[i], str);
                    str += param[i];
                }
                param.splice(first, i - first, str);
                x = first;
                first = -2;
            }
            break;

        case '.':
        case '+':
            if (1 != param[x].length)
                break;
            if (param[x+1] == '&&&') {
                assert(x < param.length-2);
                if (param[x+2]=='(' || param[x+2]=='{' || param[x+2]=='[') {
                    if (0 == level++) {
                        first = x;
                    }
                }

                str = param[x] + param[x+2];
                param.splice(x, 3, str);
            }
            if (param[x-1] == '&&&') {
                assert(x >= 2);
                str = param[x-2] + param[x];
                param.splice(x - 2, 3, str);
                x -= 2;
                if (first > x) {
                    first = x;
                }
            }
            break;
        }
    }

    if (isFunc) {
        if ('$' == param[0][0]) {
            if ('$return'==param[0] || '$=>'==param[0]) {
                cond.ret = 'return';
                param.shift();
            }
            else if ('$callback'==param[0] || '$<='==param[0]) {

            }
            else {
                cond.ret = 'function';
                if (param[0] == '$function') {
                    param.shift();
                }
                else {
                    param[0] = param[0].substr(1);
                }
            }
        }
        if (param[0] == '!') {    // execution in parallel
            cond.run = 'parallel';
            if (param.length <= 2) {
                throw(new Error('No enough parameters for ! '+param[1]));
            }
            param.shift();
            let c = param[0].charCodeAt(0);
            if (c == dashCode) {
                cond.run = 'parallel_local';
                param[0] = param[0].slice(1);
                c = param[0].charCodeAt(0);
            }
            if (c>=digitCode0 && c<=digitCode9) {
                cond.conctrl = parseInt(param[0]);
                param.shift();
            }
            c = param[0].charCodeAt(0);
            cond.set = (c == dollarCode)
                     ? translateParameter(global, cond, param, 0)
                     : param[0];
            param.shift();
        }
        if (g_parsed.funcs.hasOwnProperty(param[0])) {
            let funcRule = g_parsed.funcs[param[0]];
            let proto = funcRule.proto;
            let optarg = funcRule.optarg;
            let len = proto.length - optarg.length;
            if (param.length<len || param.length>proto.length) {
                throw new Error('Parameters mismatch: ' + param + '\n\t\t\t--> ' + funcRule.target);
            }
        }
        //else {
        //    console.log('eval', param[0]);
        //    let _func = eval(param[0]);
        //    if ("function" !== typeof _func) {
        //        throw(new Error('Not a valid function of ' + param[0]));
        //    }
        //    if (_func.length != param.length) {
        //        throw(new Error('Parameters mismatch: ' + param + '\n\t\t\t--> ' + _func));
        //    }
        //}
    }
    return true;
}

function isKeyword(str) {
    switch (str) {
        case '{{':
        case '}}':
        case '[[':
        case ']]':
        case '::':
        case '**':
        case '=>':
        case '<-':
        case '->':
        case '<--':
        case ';;':
        case '<=':
        case '//':
        case '##':
            return true;
    }
    return false;
}

let g_comment = false;
function parseLine(line) {
    let len = line.length;

    let quoteCode = ascii['\''];
    let doubleQuoteCode = ascii['"'];
    let backSlashCode = ascii['\\'];
    let spaceCode = ascii[' '];
    let tabCode = ascii['TAB'];
    let dollarCode = ascii['$'];
    let starCode = ascii['*'];
    let slashCode = ascii['/'];

    function _match(str, from, len) {
        let key5 = ['let ', 'var\t', 'new ', 'new\t', 'function ', 'function\t', 'typeof ', 'typeof\t'];
        let key4 = [' in ', ' in\t', '\tin ', '\tin\t', ' of ', ' of\t', '\tof ', '\tof\t',
            ' instanceof ', ' instanceof\t', '\tinstanceof ', '\tinstanceof\t'];
        let key3 = ['<--'];
        let key2 = ['//', '{{', '}}', '::', '**', '=>', '<-', '->', ';;', '<=', '/*', '##', '[[', ']]', '++'];
        let key1 = ['"', "'", '$', '(', ')', ' ', '\t', ',', '.', '{', '}', '\\', '[', ']', '`', '!', '+'];

        if (len >= 4) {
            for (let x = 0; x < key5.length; x++) {
                let llen = key5[x].length;
                if (len >= llen
                    && str.substring(from, from + llen) === key5[x]
                ) {
                    return 500 + llen;
                }
            }
            for (let x = 0; x < key4.length; x++) {
                let llen = key4[x].length;
                if (len >= llen
                    && str.substring(from, from + llen) === key4[x]
                ) {
                    return 400 + llen;
                }
            }
        }
        if (len >= 3) {
            for (let x = 0; x < key3.length; x++) {
                if (str.substring(from, from + 3) === key3[x]) {
                    return 300 + x;
                }
            }
        }
        if (len >= 2) {
            for (let x = 0; x < key2.length; x++) {
                if (str.substring(from, from + 2) === key2[x])
                    return 200 + x;
            }
        }
        if (len >= 1) {
            for (let x = 0; x < key1.length; x++) {
                if (str.substring(from, from + 1) === key1[x])
                    return 100 + x;
            }
        }
        return 0;
    }

    function _push(len) {
        //console.log("push %d %d %d", last, i, len);
        assert(last <= i, "last <= i");
        if (len < 0) {
            slice.push(line.substring(last, i - len));
            last = i - len;
            return;
        }
        if (last < i) {
            slice.push(line.substring(last, i));
            last = i;
        }
        if (len > 0) {
            slice.push(line.substring(i, i + len));
            i += len - 1;
            last = i + 1;
        }
    }

    let slice = []
        , expect = 0
        , last = 0
        , n
        , blank_before = true;
    for (let i = 0; i < len; i++) {
        if (g_comment) {
            if (line.charCodeAt(i) == slashCode && expect == starCode) {
                g_comment = false;
                expect = 0;
                _push(0);
            }
            else {
                expect = line.charCodeAt(i);
            }
            last = i + 1;
            continue;
        }
        if (0 != expect) {    // process string, which starts/ends with ' or "
            if (line.charCodeAt(i) != expect || line.charCodeAt(i - 1) == backSlashCode) {
                continue;
            }
            expect = 0;
            _push(-1);
            continue;
        }

        n = _match(line, i, len - i);
        //console.log("%d %s, %d,%d,%s", n, line.substring(i, len), blank_before, i, line[i-1]);
        if ((blank_before || i == 0 || line[i-1] == '=' || line[i-1] == '(' || line[i-1] == ';')
            && n >= 500
        ) {
            _push(n - 500);
            continue;
        }
        if (n<500 && n>=300) {
            _push(n>=400 ? n-400 : 3);
            continue;
        }
        if (n<300 && n>=200) {
            if (n == 200) {   // '//'
                break;        // skip all the following
            }
            if (n == 210) {   // /*
                g_comment = true;
                _push(0);
                i++;
                last = i + 1;
                continue;
            }
            if (n==203 && 0!=i) {   // if not leading ::, it may be var:type:mapped_name
                continue;
            }

            if ((n==205 || n==209)
                && (0!=i && line.substring(i-1, i)=='$' && !blank_before)
            ) {
                _push(-2);
                continue;
            }
            _push(2);
            continue;
        }
        if (105==n || 106==n) { // \t\space
            _push(0);
            last = i + 1;

            if (!blank_before && slice.length && slice[slice.length-1]=='&&&') {
                slice.pop();
            }
            blank_before = true;
            continue;
        }
        switch (n) {
        case 100:    // "
        case 101:    // '
        case 114:    // `
            expect = (100 == n) ? doubleQuoteCode : quoteCode;
            _push(0);
            break;

        case 102:   // $
            if (0!=i && '$'!=line.substring(i-1, i)) {
                _push(0);
            }
            break;

        case 103:   // (
            _push(1);
            if (0!=i && ascii.isIdentifier(line.substring(i-1, i))) {
                slice.push('$$$');
            }
            break;

        case 112:   // [
            _push(1);
            if (0 != i) {
                if (ascii.isIdentifier(line.substring(i-1, i))
                    || line.substring(i-2, i-1) == '$'
                    || line.substring(i-1, i) == ']'
                    || line.substring(i-1, i) == ')'
                ) {
                    slice.push('$$$');
                }
            }
            break;

        case 104:   // )
        case 107:   // ,
        case 109:   // {
        case 110:   // }
        case 111:   // /
        case 113:   // ]
            _push(1);
            break;

        //case 115:   // !
        case 108:   // .
        case 116:   // +
            if (line.substring(i-1, i) == '$') {
                _push(-1);
                break;
            }
            if (!blank_before) {
                _push(0);
                slice.push('&&&');
            }
            _push(1);
            slice.push('&&&');
            break;
        }
        blank_before = false;
    }
    _push(0);
    return slice;
}

function findSchemaType(sm, type) {
    switch (type) {
        case 'String':
        case 'Integer':
        case 'Number':
        case 'BOOL':
        case 'Time':
        case 'PeterID':
        case 'PeterId':
        case 'Double':
        case 'Object':
            return true;
    }
    return sm.hasOwnProperty(type);
}

function parse() {
    let global = null
        , rule = null
        , parent = null
        , chain = null
        , code = null
        , cond = null
        , save_cmd = null
        , trans_code = {
            'start': {'{{': 'CODE_BEGIN', '::': 'switch rule', '[[': 'switch schema'},
            '{{': {'}}': 'CODE_END', '<*>': 'CODE'},
            '<*>': {'<*>': 'CODE', '}}': 'CODE_END'}
        }
        , trans_schema = {
            'start': {']]': 'SCHEMA_END', '<*>': 'SCHEMA'},
            '<*>': {'<*>': 'SCHEMA', ']]': 'SCHEMA_END'}
        }
        , trans_rule = {
            'start': {'::': 'RULE'},
            '::': {'**': 'ARG', '=>': 'push chain', '<=': 'push chain'},
            '**': {'**': 'ARG', '=>': 'push chain', '<=': 'push chain'},
            '=>': {'<EOR>': '<EOR>'},
            '<=': {'<EOR>': '<EOR>'}
        }
        , trans_chain = {
            'start': {'=>': 'CHAIN', '<=': 'CHAIN'},
            '=>': {'{{': 'push begin', '<*>': 'LINE'},
            '<=': {'{{': 'push begin', '<*>': 'LINE'},
            '<-': {'{{': 'push begin', '<*>': 'LINE'},
            '<--': {'{{': 'push begin', '<*>': 'LINE'},
            '->': {'{{': 'push begin'},
            '{{': {
                '=>': 'CHAIN',
                '<=': 'CHAIN',
                '<-': 'RET',
                '->': 'COND',
                '<--': 'FINAL',
                ';;': 'pop chain',
                '<*>': 'LINE'
            },
            '<*>': {'=>': 'CHAIN', '<=': 'CHAIN', '<-': 'RET', '->': 'COND', '<--': 'FINAL', ';;': 'pop chain'}
        }
        , trans_begin = {
            'start': {'{{': 'BEGIN'},
            '{{': {'=>': 'RULE2 push chain', '<=': 'RULE2 push chain', '<*>': 'CODE', '}}': 'pop'},
            '<*>': {'<*>': 'CODE', '=>': 'RULE2 push chain', '<=': 'RULE2 push chain', ';;': 'RULE2 chain', '}}': 'pop'},
            '=>': {'<*>': 'CODE', '<EOR>': '<EOR>2'},
            '<=': {'<*>': 'CODE', '<EOR>': '<EOR>2'},
            ';;': {'<EOR>': '<EOR>2'},
            '<EOR>': {'<*>': 'CODE_}', '}}': 'pop'}
        }
        , trans = trans_code
        , tran = trans.start
        , stack = []
        , chainno = 0
        , lineno = 0
        , lines = g_parsed.lines
        ;

    function _error(err, _lineno) {
        if (!_lineno)
            _lineno = lineno;
        console.error("%d: %s", _lineno+1, lines[_lineno]);
        console.error("Error: %s", err);
        return false;
    }

    function _checkAndProcessRule(rule, checkFailed) {
        for (let i = 0; i < rule.chains.length; i++) {
            let _chain = rule.chains[i];
            //console.log(i, _chain);
            for (let x in _chain) {
                if ('chainno' == x)
                    continue;

                let _cond = _chain[x];
                let _code = _cond.code;
                if (undefined != _code && 0 == _code.length) {
                    delete _cond.code;
                }
                let _func = _cond.func;
                if (undefined != _func && 0 == _func.length) {
                    delete _cond.func;
                }

                if (('<=' == x || '<-' == x) && !_cond.func) {
                    console.log('No return value specified for\n\trule %s, chain @line %d',
                        rule.target, _chain[x].lineno);
                    return false;
                }
            }

            let cond = _chain['=>'] || _chain['<='];
            if (!(cond || (rule.embedded && 0 == i))) {
                console.log('No chain specified for\n\trule:', rule.target);
                return false;
            }
            if (!_chain.hasOwnProperty('<-') && !_chain.hasOwnProperty('->')) {
                if (0 == i) {       // for parameter checking
                    _chain['<-'] = {
                        func: ['$callback', '__err', 'null']
                    };
                    continue;
                }
                cond = rule.chains[i-1]['=>'];
                if (cond) {
                    let code = [];
                    for (let x in cond.func) {
                        code.push(cond.func[x].split('\'').join('\\\''));
                    }
                    _chain['<-'] = {
                        func: ['$callback', '__err', '__arg'],
                        code: ['console.log(\'Warning: error in default callback of ['
                              + code.join(' ') + ']:\', __err);']
                    };
                }
                else {
                    assert(rule.embedded);
                    _chain['<-'] = {
                        func: ['$callback', '__err', '__arg'],
                        code: ['console.log(\'Warning: error in default callback of ['
                              + rule.target.split('\'').join('\\\'') + ']:\', __err);']
                    };
                }
            }
        }
        return true;
    }

    function _setDefaultRet(rule, cond) {
        for (let i = 0; i < rule.chains.length; i++) {
            let cc = rule.chains[i];
            if (!cc.hasOwnProperty('<-') && !cc.hasOwnProperty('->')) {
                cc['<-'] = cond;
            }
        }
    }

    function _genRule(param) {
        let _export = false;
        let funcname = param[0];
        if (param[0].substring(0, 1) == '/') {
            param[0] = param[0].substring(1);
            if (param[0] == '') {
                param.splice(0, 1);
            }
            funcname = param[0];
            if (param[0] == '_') {
                param[0] = '__x';
                param.unshift('__arg');
                param.unshift('_');
            }
            else {
                let arr = param[0].split('/');
                if (arr.length> 1) {
                    param[0] = arr.join('__');
                }
            }
            _export = true;
        }
        if (g_parsed.funcs.hasOwnProperty(param[0])) {
            throw new Error('Function with same name: ' + param[0] + ' @line ' + lineno);
        }
        checkFuncString(global, null, param, false);

        let target = param.join(' ');//param[0] + '(' + param.slice(1).join(',') + ')';
        let rule = {lineno: lineno, target: target, chains: [], rules: [], export: _export};
        let proto = [{name: param[0], display: funcname}];
        let optarg = [];

        for (let i=1; i<param.length; i++) {
            let name = param[i];
            let arg = {display: name};
            if (name.substr(0, 1) == '?') {
                name = name.substr(1);
                arg.optional = true;
            }
            else if (optarg.length > 0) {
                throw new Error('Only the last parameters can be optional: ' + rule.target + ' @line ' + lineno);
            }
            if (name.substr(0, 1) == '=') {
                name = name.substr(1);
                arg.default = true;
            }
            switch (name.substr(0, 1)) {
            case '%':
                name = name.substr(1);
                if (name == '') {
                    name = '__http_cookies';
                }
                else {
                    name = name.toLowerCase();
                }
                arg.cookies = true;
                break;
            case '&':
                name = name.substr(1);
                if (name == '') {
                    name = '__http_files';
                }
                else {
                    name = name.toLowerCase();
                }
                arg.files = true;
                break;
            case '^':
                name = name.substr(1);
                if (name == '') {
                    name = '__http_headers';
                }
                else {
                    name = name.toLowerCase();
                }
                arg.headers = true;
                break;
            case '*':
                name = name.substr(1);
                if (name == '') {
                    name = '__http_request';
                }
                else {
                    name = name.toLowerCase();
                }
                arg.request = true;
                break;
            case '>':
                if (name.length != 1) {
                    throw new Error('> (http_response) can only be used alone, @line ' + (lineno-1));
                }
                name = '__http_response';
                break;
            }

            // var:type:property_name
            let n = name.search(/\:/);
            if (-1 != n) {
                let type = name.substr(n + 1);
                name = name.substr(0, n);

                n = type.search(/\:/);
                if (-1 != n) {
                    let property = type.substr(n + 1);
                    if (property) {
                        arg.property = property;
                    }
                    type = type.substr(0, n);
                }
                if (type) {
                    arg.type = type;
                }
            }
            arg.name = name;
            if (!arg.property) {
                arg.property = name;
            }
            if (arg.optional) {
                optarg.push(name);
            }
            proto.push(arg);
        }
        rule.proto = proto;
        rule.optarg = optarg;
        return rule;
    }

    function _genEmbeddedRule(param) {
        let cond = {func: param};
        let rule = {embedded: true, target: param.join(' '), chains: [{}], rules: []};

        if (undefined == param || !checkFuncString(global, cond, param, true)) {
            throw new Error('";;" Must be following a valid function. ' + param.join(' ') + ' @line ' + (lineno-1));
        }
        if (!cond.ret) {
            throw new Error('Embedded rule must start with "$return" or "$function". ' + param.join(' ') + ' @line ' + (lineno-1));
        }
        rule.proto = cond;
        return rule;
    }

    function _proc(cmd, action, param) {
        if (undefined == action) {
            action = tran[cmd];
            if (undefined == action) {
                return _error(sprintf('Not expected. "%s" %s', cmd, JSON.stringify(tran)));
            }
        }
        //console.log("Proc %s, %s", cmd, action);
        tran = trans[cmd];
        switch (action) {
            case 'CODE_BEGIN':
                code = g_parsed.code;
                break;

            case 'CODE_END':
                tran = trans.start;
                code = null;
                break;

            case 'switch schema':
                trans = trans_schema;
                tran = trans.start;
                break;

            case 'SCHEMA':
                checkFuncString(global, null, param, false);
                g_parsed.schema.push(param.join(''));
                break;

            case 'SCHEMA_END':
                trans = trans_code;
                tran = trans.start;
                break;

            case 'CODE':
                checkFuncString(global, null, param, false);
                code.push(param);
                break;

            case 'CODE_}':
                if ('}'!=param[0] && 'case'!=param[0] && 'default:'!=param[0] && 'default'!=param[0]) {
                    let last = parent.rules[parent.rules.length-1];
                    if (last.proto.ret != 'function') {
                        return _error('No more code is allowed after a rule');
                    }
                }
                code.push(param);
                break;

            case 'switch rule':
                trans = trans_rule;
                tran = trans.start;
                return _proc('::', undefined, param);

            case 'switch code':
                trans = trans_code;
                tran = trans.start;
                return _proc('{{', undefined, param);

            case 'RULE':
                assert(null == rule);

                if (0 == param.length) {
                    return _error('No parameters');
                }
                rule = _genRule(param);

                g_parsed.rules.push(rule);
                g_parsed.funcs[param[0]] = rule;
                global = rule;
                parent = null;
                break;

            case 'ARG':
                checkFuncString(global, null, param, false);
                if (!procArg(rule, param)) {
                    return _error('No parameters');
                }
                break;

            case 'CHAIN':
                chain = {chainno: chainno};
                code = [];
                cond = {lineno: lineno+1, code: code};
                chain[cmd] = cond;
                rule.chains.push(chain);
                chainno ++;
                save_cmd = cmd;

                if (0 != param.length) {
                    return _proc('<*>', undefined, param);
                }
                break;

            case 'COND':
            case 'RET':
                if (chain.hasOwnProperty('->') || chain.hasOwnProperty('<-'))
                    return _error('Only one of "->" "<-" condition can be provided');
            // else pass through
            case 'FINAL':
                if (chain.hasOwnProperty('<--'))
                    return _error('Only one "<--" condition can be provided');
                code = [];
                cond = {lineno: lineno+1, code: code};
                chain[cmd] = cond;
                save_cmd = cmd;
                if (0 != param.length)
                    return _proc('<*>', undefined, param);
                break;

            case 'LINE':
                assert(undefined == cond.func || 0 == cond.func.length);
                switch (save_cmd) {
                case '=>':
                    if (!checkFuncString(rule, cond, param, true) || cond.ret) {
                        return _error('Not valid function');
                    }
                    cond.func = param;
                    break;

                case '<-':
                case '<=':
                    if (!checkFuncString(rule, cond, param, false)) {    // TODO
                        return _error('No valid parameters');
                    }
                    cond.func = param;
                    break;

                case '<--':
                    if (!checkFuncString(rule, cond, param, false)) {    // TODO
                        return _error('No valid parameters');
                    }
                    cond.func = param;
                    _setDefaultRet(rule, cond);
                    break;

                case '->':
                    return _error('Can\'t specify chain for ->, which must be following a {{ }} code block');
                }
                break;

            case '<EOR>':
                if (!_checkAndProcessRule(rule, true))
                    return false;
                rule = null;
                chain = null;
                cond = null;
                code = null;
                trans = trans_code;
                tran = trans.start;
                break;

            case '<EOR>2':
                if (!_checkAndProcessRule(rule, false))
                    return false;
                rule = null;
                chain = null;
                cond = null;
                break;

            case 'RULE2 push chain':
                assert(null != code);
                assert(null == rule);
                assert(null != parent);

                let func = code.pop();
                rule = _genEmbeddedRule(func);
                parent.rules.push(rule);
                code.push(rule);

            // pass through
            case 'push chain':
                // save and reset environments
                stack.push(trans);
                stack.push(tran);
                stack.push(rule);
                stack.push(parent);
                stack.push(chain);
                stack.push(cond);
                stack.push(code);
                stack.push(save_cmd);
                trans = trans_chain;
                tran = trans.start;
                return _proc(cmd, undefined, param);

            case 'pop chain':
                // restore environments
                save_cmd = stack.pop();
                code = stack.pop();
                cond = stack.pop();
                chain = stack.pop();
                parent = stack.pop();
                rule = stack.pop();
                tran = stack.pop();
                trans = stack.pop();
                return _proc('<EOR>', undefined, param);

            case 'RULE2 chain':
                assert(null != code);
                assert(null == rule);
                assert(null != parent);

                let func = code.pop();
                rule = _genEmbeddedRule(func);
                parent.rules.push(rule);
                code.push(rule);
                return _proc('<EOR>', undefined, param);

            case 'push begin':
                assert(chain[save_cmd] == cond, '1');
                assert(code == cond.code, '2');

                stack.push(trans);
                stack.push(tran);
                stack.push(rule);
                stack.push(parent);
                stack.push(chain);
                stack.push(cond);
                stack.push(code);
                stack.push(save_cmd);
                trans = trans_begin;
                tran = trans.start;
                parent = rule;
                rule = null;
                chain = null;
                cond = null;
                return _proc('{{', undefined, param);

            case 'pop':
                // restore environments
                save_cmd = stack.pop();
                code = stack.pop();
                cond = stack.pop();
                chain = stack.pop();
                parent = stack.pop();
                rule = stack.pop();
                tran = stack.pop();
                trans = stack.pop();
                return true;
        }
        return true;
    }

    let param
        , save = null;

    function _flush(which) {
        function _pp(which) {
            let cmd = isKeyword(which[0]) ? which.shift() : '<*>';
            return _proc(cmd, undefined, which);
        }

        for (let i = 0; i < which.length; i++) {
            if (which[i] == '{{' || which[i] == '}}') {
                if (!_pp(which.splice(0, 0 == i ? 1 : i)))
                    return false;
                i = -1;
            }
        }
        return (0 != i) ? _pp(which) : true;
    }

    for (; lineno<lines.length; lineno++) {
        param = parseLine(lines[lineno]);
        assert('//' != param[0]);
        if (0 == param.length || '' == param[0]) {
            if (null != save) {
                _flush(save);
                save = null;
            }
            continue;
        }
        if ('##' == param[0]) {
            //console.log(param);
            param.shift();

            if (param.length > 0) {
                if (param[0].substring(0, 1) == '$') {
                    let what = param[0].substring(1);
                    param.shift();

                    let whatjson = param.join('');
                    try {
                        g_parsed.options[what] = utils.parseJSON(whatjson, true);
                    }
                    catch (e) {
                        console.log('Fail to parse', whatjson, 'at line', lineno+1);
                        throw e;
                    }
                }
                else {
                    g_parsed.shell = g_parsed.hasOwnProperty('shell')
                                   ? g_parsed.shell.concat(param)
                                   : param;
                }
            }
            continue;
        }

        if ('\\' == param[param.length - 1]) {
            param.pop();
            save = (null == save) ? param : save.concat(param);
        }
        else if (null != save) {
            if (!_flush(save.concat(param)))
                return false;
            save = null;
        }
        else if (!_flush(param)) {
            return false;
        }
    }
    if (null != rule) {
        // to end the last rule
        _proc('start', '<EOR>', param);
    }
    assert(null == code);

    //printRule(g_parsed.rules[0]);
    return true;
}

function printRule(rule) {
    console.log(JSON.stringify(rule, null, ' '));
}

function genRet(rule, arg, which, lineno) {
    if (arg instanceof Array) {
        if ('$callback'==arg[0] || '$<='==arg[0]) {
            arg.shift();
        }
        else if ('null'!=arg[0] && '$?'!=arg[0] && '\''!=arg[0].substring(0, 1)
            && -1==arg[0].search('err') && -1==arg[0].search('Err')
        ) {
            console.log('\n\nWarning: the first arg of callback seems not a valid error msg: %s@line %d\n\n',
                arg[0], lineno);
        }
        assert(arg.length >= 1);

        let first = true;
        let str = (0 == which) ? 'return process.nextTick(function() {' : 'return ';
        str += 'callback(';
        for (let x = 0; x < arg.length; x++) {
            if (',' != arg[x]) {
                if (!first)
                    str += ',';
                str += arg[x];
                first = false;
            }
        }
        str += (0 == which) ? ');});' : ');';
        return str;
    }
    return 0 == which
         ? 'return process.nextTick(function() {' + arg + ';});'
         : 'return ' + arg + ';';
}

function genCheck(rule, ret) {
    let notnull = rule.notnull;
    if (undefined == notnull)
        return '';
    assert(ret, 'No return value specified for parameter checking');

    let str = '';
    for (let x in notnull) {
        str += sprintf('if (undefined==%s || null==%s) {\nlet __err=\'Invalid parameters\'; %s}\n',
            notnull[x], notnull[x], genRet(rule, ret.func, 0, -1)
        );
    }
    return str;
}

function genCode(rule, code, which) {
    if (undefined == code)
        return '';

    let str = ''
        , line;
    for (let x in code) {
        line = code[x];
        if (line instanceof Array) {
            if ('$callback'==line[0] || '$<='==line[0]) {
                str += genRet(rule, line, which, -1);
            }
            else {
                for (let y in line) {
                    str += line[y] + ' ';
                }
                str += '\n';
            }
        }
        else if ('string' == typeof line) {
            str += line;
        }
        else {
            // embeded rule
            str += genRule2(line);
        }
    }
    return str;
}

function _genFuncCallStr(func, ret, conctrl) {
    let str, i;
    if (conctrl) {
        let arr = func[0].split('.');
        if (arr.length > 1) {
            let f = arr.pop();
            let o = arr.join('.');
            str = conctrl + '.run(' + arr.join('.') + ',' + o + '.' + f;
        }
        else {
            str = conctrl + '.run(' + func[0];
        }
        if (func.length > 1) {
            for (i = 1; i < func.length; i++) {
                if (func[i] != ',') {
                    str += ',';
                    break;
                }
            }
        }
        else {
            str += ',';
        }
    }
    else {
        switch (ret) {
        case 'return':
            assert(func.length >= 1, 'Wrong format of func: ' + func.toString());
            str = 'return ' + func[0] + '(';
            i = 1;
            break;

        case 'function':
            str = 'function ' + func[0] + '(';
            i = 1;
            break;

        case undefined:
            str = func[0] + '(';
            i = 1;
            break;
        }
    }
    for ( ; i < func.length; i++) {
        if (func[i] != ',') {
            str += func[i] + ',';
        }
    }
    return str;
}

function appendGlobalCode(code) {
    g_parsed.code.push(code);
}

function appendGlobal(lines) {
    //Array.prototype.push.apply(g_parsed.lines, lines);
    //console.log('append', lines);
    for (let x in lines) {
        let str = lines[x].trim();
        if (str.substring(0, 2) == '##')        // TODO
            continue;
        g_parsed.lines.push(lines[x]);
    }
}

let ctrlId = 0;
function genParallel(rule, cond, func, which, last) {
    let code = '';
    if (cond.conctrl) {
        if ('parallel' == cond.run) {
            appendGlobalCode('let __con' + ctrlId + '=Concurrent.create(' + cond.conctrl + ', \' !' + func[0] + '\');\n');
        }
        else {
            code = 'let __con' + ctrlId + '=Concurrent.create(' + cond.conctrl + ');\n';
        }
    }
    return code + 'let __n, __i, __each, __results, __ret=0, __target=' + cond.set + ';'
        + 'if (__target instanceof Array) {'
        + '__results=[];__n=Object.keys(__target).length;__i=0;'
        + '} else {'
        + 'assert(__target, "Parallel target is null/undefined: '+_genFuncCallStr(func, cond.ret)+'");'
        + '__results={};__n=Object.keys(__target).length;__i=0;}'
        + 'for (__each in __target) {'
        + 'function __encap(__x, __ind, __y) {'
        + (cond.conctrl ? _genFuncCallStr(func, cond.ret, '__con' + ctrlId++) :  _genFuncCallStr(func, cond.ret))
        + 'function(err, arg) {'
        + '__results[__x] = [err, arg];'
        + 'assert(__ret<__n); __ret++;'
        + 'if (__ret==__n) {'
        + (last ? genRet(rule, 'callback(null, __results)', which+1, cond.lineno) : '__next(null, __results);')
        + '}});}'
        + '__encap(__each, __i++, __target[__each]);}'
        + 'if (0==__n) {'
        + (last ? genRet(rule, 'callback(null, __results)', which, cond.lineno) : '__next(null, __results);')
        + '}'
        + (!last ? ('function __next(__err, __arg) {' + genOk(rule, which+1) +'}') : '')
        + (cond.ret ? 'return;' : '');
}

function genFuncCall(rule, cond, func, which, last) {
    if (null == cond)
        return '';

    if ('parallel' == cond.run || 'parallel_local' == cond.run) {
        return genParallel(rule, cond, func, which, last);
    }
    else if ('function' == cond.ret) {
        return _genFuncCallStr(func, cond.ret) + 'callback) {'
            +   genOk(rule, which+1)
            + '}';
    }

    return last
        ? _genFuncCallStr(func, cond.ret) + 'callback);'
        : _genFuncCallStr(func, cond.ret)
            +  'function(__err, __arg) {if (null == __err) {'
            +   genOk(rule, which+1)
            +   '}'
            +   genNo(rule, which+1)
            +   '});'
        ;
}

function genOk(rule, which) {
    assert(which < rule.chains.length);
    let chain = rule.chains[which]
      , cond = chain['=>']
      , str;

    if (cond) {
        str = genCode(rule, cond.code, which);
        let func = cond.func;
        if (func) {
            if ('$callback'==func[0] || 'callback'==func[0]) {
                str += genRet(rule, func, which, cond.lineno);
                assert(which == rule.chains.length-1
                    , sprintf('More chain found after LAST => chain @line %d. Something must be wrong!', cond.lineno));
            }
            else {
                if ('null'==func[0] || '\''==func[0].substr(0, 1) || '"'==func[0].substr(0, 1)) {
                    throw new Error(func[0] + ' seems not a valid function, should be <= ? @line '
                                    + cond.lineno + ', Rule: ' + rule.target);
                }
                str += genFuncCall(rule, cond, func, which, which == rule.chains.length-1);
            }
        }
        else {
            assert(which == rule.chains.length-1
                , sprintf('More chain found after LAST => chain @line %d. Something must be wrong!', cond.lineno));
        }
        return str;
    }

    cond = chain['<='];
    assert(cond && cond.func);
    str = genCode(rule, cond.code, which);
    str += genRet(rule, cond.func, which, cond.lineno);

    assert(which == rule.chains.length-1
        , sprintf('More chain found after <= chain @line %d. Something must be wrong!', cond.lineno));
    return str;
}

function genNo(rule, which) {
    assert(which < rule.chains.length);
    let chain = rule.chains[which]
        , cond = chain['<-'];

    if (cond) {
        let str = 'else {\n';
        str += genCode(rule, cond.code, which);
        str += genRet(rule, cond.func, which, cond.lineno);
        str += '}\n';
        return str;
    }
    cond = chain['->'];
    if (cond) {
        let str = 'else {\n';
        str += genCode(rule, cond.code, which);
        str += '}\n';
        return str;
    }
    return '';
}

function genRule(rule) {
    let chain = rule.chains[0];

    let str = genCheck(rule, chain['<-']);
    str += genOk(rule, 0);
    return str;
}

function genRule2(rule) {
    let cond = rule.proto;
    return genFuncCall(rule, cond, cond.func, 0, 1==rule.chains.length);
}

function genOptionalArgCheck(optarg) {
    let str = 'if (undefined == callback) {';

    for (let i=optarg.length-1; i>=0; i--) {
        str += 'if (\'function\' == typeof ' + optarg[i] +') { callback=' + optarg[i] +';';
        for (let j=optarg.length-1; j>=i; j--) {
            str += optarg[j] + '=undefined;';
        }
        str += '} else { assert(undefined == ' + optarg[i] + ');'
    }
    for (let i=0; i<optarg.length; i++) {
        str += '}';
    }
    str += '}';

/*    let str = '';
    for (let i=0; i<optarg.length; i++) {
        str += 0 == i
            ? ('if(\'function\'==typeof ' + optarg[i] + ') {')
            : ('else if(\'function\'==typeof ' + optarg[i] + ') {');
        for (let j=i+1; j<optarg.length; j++) {
            str += 'assert(undefined==' + optarg[j] + ', \'optional arg assertion failed: \' + ' + optarg[j] + ');';
        }
        str += 'assert(undefined==callback, \'optional arg assertion failed: \' + callback);'
             + 'callback=' + optarg[i] + ';' + optarg[i] + '=undefined;}';
    }*/
    return str;
}

function genTypeCheck(proto) {
    let str = '';
    for (let i=1; i<proto.length; i++) {
        switch (proto[i].type) {
        case undefined:
        case '':
            break;

        case 'Integer':
        case 'Number':
        case 'Double':
            str += proto[i].name + ' = +' + proto[i].name + ';\n'
                + 'if (isNaN(' + proto[i].name + ')) {'
                + genRet(null, 'callback(\'Invalid arguments\', \''+proto[i].name+'\')', 0)
                + '}\n';
            break;

        case 'String':
        case 'BOOL':
        case 'Time':
        case 'PeterID':
        case 'PeterId':
        case 'Object':
            break;

        default:
            str += 'if (!Parser.check(__runtime_schema__[\'' + proto[i].type + '\'], ' + proto[i].name + ')) {'
                + genRet(null, 'callback(\'Invalid arguments\', \''+proto[i].name+'\')', 0)
                + '}\n';
        }
    }
    return str;
}

function _genShell(phrases, rule) {
    let str = ''
      , what;

    for (let x in phrases) {
        what = phrases[x];

        if (what.substring(0, 1) == '$') {
            let n = parseInt(what.substring(1));
            if (!isNaN(n)) {
                if (n >= rule.proto.length) {
                    throw new Error("Wrong parameters: " + what);
                }
                str += rule.proto[n].name;
            }
            else if ('$@' == what) {
                str += '\n' + genRule(rule);
            }
            else if ('$+' == what) {
                for (let i=1; i<rule.proto.length; i++) {
                    str += rule.proto[i].name + ',';
                }
            }
            else if ('$!?' == what) {
                if (0 != rule.optarg.length) {
                    str += genOptionalArgCheck(rule.optarg);
                }
                str += 'assert(callback, \'Invalid callback of [' + rule.target + '], possibly missing arguments\');';
            }
            else if ('$!:' == what) {
                str += genTypeCheck(rule.proto);
            }
        }
        else {
            str += what;
        }
    }
    return str + '\n';
}

function genShell(shell, rule) {
    let phrases = parseLine(shell);
    return _genShell(phrases, rule);
}

let g_CRLF = (function() {
    let type = os.type();
    if (type == 'Windows_NT') {
        return '\r\n';
    }
    return '\n';
})();

function prepareSchema() {
    let schema = {};
    let code = '';
    if (g_parsed.schema.length) {
        let str = g_parsed.schema.join('\n');
        schema = Parser.parse(Buffer.from(str));
        code = 'let __runtime_schema__ = Parser.parse(Buffer.from(\'' + g_parsed.schema.join('\\n') + '\'));\n';
    }

    for (let x in g_parsed.rules) {
        let proto = g_parsed.rules[x].proto;
        for (let i in proto) {
            if (proto[i].type) {
                if (!findSchemaType(schema, proto[i].type)) {
                    throw new Error('Unknown type of \'' + proto[i].type + '\' in [' + g_parsed.rules[x].target + ']');
                }
            }
        }
    }
    return code;
}

function generateFromString(str, shell, filename) {
    try {
        let lines = str.split(g_CRLF);
        g_parsed = {path: filename, code: [], schema: [], rules: [], funcs: {}, lines: lines, options: {}};
        if (parse()) {
            let code = '';
            for (let i in g_parsed.rules) {
                code += (undefined != shell)
                    ? genShell(shell, g_parsed.rules[i])
                    : (g_parsed.shell
                        ? _genShell(g_parsed.shell, g_parsed.rules[i])
                        : genShell('function $0($+ callback) { $!? $!: $@ }', g_parsed.rules[i])
                    );
            }
            code = prepareSchema() + genCode(null, g_parsed.code, 0) + code;
            return code;
        }
    }
    catch (e) {
        throw e;
    }
    return null;
}

function generate(file, shell) {
    let filename = Path.resolve(process.cwd(), file);
    let str = fs.readFileSync(filename).toString();
    str = generateFromString(str, shell, filename);
    if (str) {
        str = beautify(str);
        if (g_parsed.options.save == '1') {
            fs.writeFileSync(filename + '-gen.js', str);
        }
    }
    return str;
}

module.exports = {
    generate: generate,
    generateFromString: generateFromString,
    getLastParsed: function (err, arg) {
        return g_parsed;
    }
};
