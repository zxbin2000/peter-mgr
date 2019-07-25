/**
 * Created by linshiding on 3/10/15.
 */

let assert = require('assert');
let ascii = require('../utils/ascii');
let ObjectID = require('mongodb').ObjectID;

let spaceCode = ascii[' '];
let tabCode = ascii['\t'];
let returnCode = ascii['\n'];
let carryCode = ascii['\r'];
let slashCode = ascii['/'];
let commaCode = ascii[','];
let semicolonCode = ascii[';'];
let colonCode = ascii[':'];
let leftBraceCode = ascii['{'];
let rightBraceCode = ascii['}'];
let atCode = ascii['@'];
let starCode = ascii['*'];
let leftBracketCode = ascii['['];
let rightBracketCode = ascii[']'];
let tildeCode = ascii['~'];
let exclamCode = ascii['!'];
let hashCode = ascii['#'];
let digitCode0 = ascii['0'];
let digitCode9 = ascii['9'];
let quoteCode = ascii['\''];
let doubleQuoteCode = ascii['"'];
let dotCode = ascii['.'];
let pCode = ascii['p'];
let plusCode = ascii['+'];
let alphaCode_a = ascii['a'];
let alphaCode_z = ascii['z'];
let alphaCode_A = ascii['A'];
let alphaCode_Z = ascii['Z'];

function printSchema(schema, level) {
    let str = "";
    let sch, type;

    let tab = "";
    for (let i = 0; i < level; i++) tab += "\t";

    for (let x in schema) {
        sch = schema[x];

        type = sch.__type__;
        if (undefined == type) {  // __ attributes
            switch (x) {
                case '__str__':
                    break;

                case '__ref__':
                    str += tab + x + " : ";
                    for (let y in sch) {
                        str += y + ", ";
                    }
                    str += "\n";
                    break;

                case '__ziplist__':
                    str += tab + x + " : ";
                    for (let y in sch) {
                        str += JSON.stringify(sch[y]) + ", ";
                    }
                    str += "\n";
                    break;

                default:
                    str += tab + x + " : " + sch + "\n";
                    break;
            }
        }
        else {
            switch (x) {
                case '__up__':
                    str += tab + x + " : " + sch.__name__ + "\n";
                    break;

                case '__inherit__':
                    str += tab + x + " -> " + sch.__name__ + "\n";
                    break;

                case '__to__':
                    str += tab + x + " -> " + sch.__up__.__name__ + "." + sch.__name__ + "\n";
                    break;

                default:
                    str += tab + x + " : {\n" + printSchema(sch, level + 1) + tab + "}\n";
                    if (0 == level)
                        str = str + '\n';
                    break;
            }
        }
    }

    return str;
}

function isKeyword(str) {
    switch (str) {
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
    return false;
}

function getRealType(str) {
    switch (str) {
        case 'String':
        case 'Integer':
        case 'Number':
        case 'BOOL':
        case 'Time':
        case 'PeterID':
        case 'PeterId':
        case 'Double':
        case 'Object':
            return str;
        case 'TRUE':
        case 'True':
        case 'true':
        case 'FALSE':
        case 'False':
        case 'false':
            return 'BOOL';
        case 'NOW':
        case 'Now':
            return 'Time';
    }

    if (ascii.isNumber(str))
        return 'Integer';
    if (ascii.isString(str))
        return 'String';

    return null;
}

function _addToList(who, list, what) {
    if (who.hasOwnProperty(list)) {
        who[list].push(what);
    }
    else {
        who[list] = [what];
    }
}

function _processDefault(schema, attr, type, value) {
    switch (type) {
        case 'String':
            attr['__default__'] = value.substring(1, value.length - 1);
            break;

        case 'Integer':
        case 'Number':
            attr['__default__'] = parseInt(value);
            break;

        case 'Time':
            assert(value.toLowerCase() == 'now');
            attr['__default__'] = '$NOW';
            break;

        case 'BOOL':
            attr['__default__'] = (value.toLowerCase() == 'true');
            break;

        default:
            assert(false);
            break;
    }
    attr.__type__ = type;
    _addToList(schema, '__defaultlist__', attr.__name__);
}

function genPair(which, name, type) {
    let cur = which[1][which[0]];
    let isSet = false;
    let key = null;
    let sch = {__type__: type};

    switch (name.charCodeAt(0)) {
        case starCode:
            name = name.substring(1);
            sch.__required__ = true;
            break;
        case plusCode:
            name = name.substring(1);
            sch.__zip__ = true;
            break;
    }

    switch (name.charCodeAt(0)) {
        case atCode:  // @
            if (0 != which[0])   // Peter can only be defined outside
                return false;
            sch.__type__ = '__peter__';
            break;

        case starCode:  // *
            return null;

        case leftBracketCode:  // [
            let len = name.length;
            if (name.charCodeAt(len - 1) != rightBracketCode  // ]
                || name.charCodeAt(1) == tildeCode              // ~
                || name.charCodeAt(1) == starCode               // *
                || name.charCodeAt(1) == leftBracketCode)       // [
                return null;

            isSet = true;
            sch.__type__ = '__list__';
            sch.__element__ = type;

            for (let i = 1; i < len - 1; i++) {
                if (name.charCodeAt(i) == exclamCode) { // !
                    key = name.substring(i + 1, len - 1);
                    name = name.substring(0, i);
                    name = name + ']';

                    if (key != '') {
                        sch.__type__ = '__keyed__';
                        sch.__key__ = key;
                    }
                    else {
                        sch.__type__ = '__set__';
                    }
                    break;
                }
            }
            break;

        case leftBraceCode:  // {
            let len = name.length;
            if (name.charCodeAt(len - 1) != rightBraceCode  // }
                || name.charCodeAt(1) == tildeCode              // ~
                || name.charCodeAt(1) == starCode               // *
                || name.charCodeAt(1) == leftBraceCode)       // {
                return null;

            isSet = true;
            sch.__type__ = '__obj__';
            sch.__element__ = type;
            break;

        case tildeCode: // ~
            if (sch.__required__)                 // can't be a required member, link can be defaulted
                return null;

            if ('~' == name && type.charCodeAt(0) == atCode) {
                name = '~' + type;
            }

            sch.__type__ = '__link__';
            sch.__element__ = type;
            break;

        case hashCode:  // #
            // TODO
            break;

        case pCode:
            if ('peer' == name) {
                let parent = cur.__up__;
                if ('__link__' == cur.__type__
                    && '~' == cur.__name__
                    && '__container__' == cur.__element__
                    && atCode == type.charCodeAt(0)
                ) {
                    cur.__name__ = '~' + type;
                    if (parent.hasOwnProperty(cur.__name__)) {
                        console.log("Duplicated attribute %s.%s", parent.__name__, cur.__name__);
                        return null;
                    }
                    parent[cur.__name__] = cur;
                }
            }
            break;
    }

    if (cur.hasOwnProperty(name)) {
        if (0!=which[0] || name.substring(0, 1)=='@') {           // TODO: FIXIT
            console.log("Duplicated attribute %s.%s %d", cur.__name__, name, which[0]);
            return null;
        }
    }

    if (sch.__required__) {
        _addToList(cur, '__requirelist__', name);
    }
    if (sch.__zip__) {
        _addToList(cur, '__ziplist__', name);
        if (cur.__zip__) {
            console.log("Can't support embedded zip member %s inside +%s", name, cur.__name__);
            return null;
        }
    }

    if ('~' != name) {      // deferred to installSchema
        cur[name] = sch;
    }
    sch.__up__ = cur;
    sch.__name__ = name;

    switch (sch.__type__) {
        case '__link__':
        case '__set__':
        case '__keyed__':
        case '__list__':
        case '__obj__':
            /*console.log("TO_SET_TYPE: %s %s.%s", sch.__element__, cur.__name__, name);
             if (sch.__element__ == which[1][1].__name__ && sch.__element__.charCodeAt(0) != atCode) { // self-embedding not allowed
             return null;
             }*/
            which[2].push([cur, sch, which[1][1]]);
            break;

        case '__peter__':
        case '__container__':
            break;

        default:
            assert(type == sch.__type__);

            let str = getRealType(type);
            if (str == null) {
                /*console.log("TO_SET_TYPE: %s %s.%s", sch.__type__, cur.__name__, name);
                 if (sch.__type__ == which[1][1].__name__ && sch.__type__.charCodeAt(0) != atCode) { // self-embedding not allowed
                 return null;
                 }*/
                which[2].push([cur, sch, which[1][1]]);
                break;
            }
            else if (str != type) {       // that means the attribute has default value
                if (sch.__required__ == true) {
                    console.error("Error: required attribute '%s' can't be defaulted", sch.__name__);
                    return null;
                }
                _processDefault(cur, sch, str, type);
            }
            break;
    }
    return sch;
}

function enterContainer(which, name, type, inherit) {
    let sch = genPair(which, name, type=='[' ? '__array__' : '__container__');
    if (null == sch)
        return null;

    which[0]++;
    which[1][which[0]] = sch;
    if (null != inherit) {
        assert(atCode == inherit.charCodeAt(0));
        which[3][sch.__name__] = inherit;
    }
    return sch;
}

function leaveContainer(which, name, type) {
    assert(0 < which[0]);
    let sch = which[1][which[0]];
    which[0]--;

    assert(type=='}' && (sch.__type__=='__container__' || sch.__type__=='__peter__' || sch.__element__=='__container__')
        || type==']' && sch.__element__=='__array__', type + ' ' + sch.__type__);
    let parent = which[1][which[0]];
    if (sch.__ziplist__ && 0!=which[0]) {
        _addToList(parent, '__ziplist__', sch.__name__);
    }
    return parent;
}

function parseData(data) {
    assert(data instanceof Buffer);

    let i = 0, start = -1;
    let single = null;
    let cur = {};
    let which = [
        0,        // level
        [cur],    // stack
        [],       // untyped
        {}        // inheritance
    ];

    function _isBlank(c) {
        return c == spaceCode
            || c == tabCode
            || c == returnCode
            || c == carryCode
            || c == commaCode
            || c == semicolonCode;
    }

    function _getToken() {
        let str;
        if (single) {
            str = single;
            single = null;
            return str;
        }

        if (i >= data.length)
            return null;
        while (i < data.length && _isBlank(data[i])) {
            i++;
        }
        if (i >= data.length)
            return null;
        if (start == -1)
            start = i;

        let b = i;
        let expect = 0;
        while (i < data.length) {
            if (0 != expect) {
                if (data[i] == expect) {
                    i++;
                    break;
                }
                i++;
                continue;
            }
            if (data[i] == quoteCode || data[i] == doubleQuoteCode) {
                expect = data[i];
                i++;
                continue;
            }

            if (_isBlank(data[i]))
                break;

            if (data[i] == slashCode
                || data[i] == leftBraceCode
                || data[i] == rightBraceCode
                || (colonCode == data[i] && colonCode != data[i + 1] && colonCode != data[i - 1])
            ) {
                single = data.toString('utf8', i, i + 1);
                i++;
                break;
            }
            i++;
        }

        if (single != null) {
            if (i == b + 1) {
                str = single;
                single = null;
            }
            else {
                str = data.toString('utf8', b, i - 1);
            }
        }
        else {
            str = data.toString('utf8', b, i);
        }
        return str;
    }

    function _seekToEOL() {
        while (i < data.length && data[i] != returnCode && data[i] != carryCode) {
            i++;
        }
    }

    function _error() {
        console.error("%s", data.toString('utf8', 0, i));
        console.error("===================\nError: Invalid format");
        return false;
    }

    let name = null
        , colon = false
        , inherit = null;

    function _onToken(token) {
        //console.log("==%s==", token);
        switch (token) {
            case ':':
                if (null == name || colon) {
                    return _error();
                }
                colon = true;
                break;

            case '{':
            case '[':
                if (!colon || null == (cur = enterContainer(which, name, token, inherit))) {
                    return _error();
                }

                name = null;
                inherit = null;
                colon = false;
                break;

            case '}':
            case ']':
                if (null != name
                    || colon
                    || null == (cur = leaveContainer(which, name, token))
                ) {
                    return _error();
                }
                if (0 == which[0]) {
                    which[1][1]['__str__'] = data.toString('utf8', start, i);
                    start = -1;
                }
                break;

            default:
                if (null == name) {
                    if (colon)
                        return _error();
                    name = token;
                    break;
                }
                if (!colon) {
                    return _error();
                }
                // process inheritance
                if (atCode == name.charCodeAt(0) && atCode == token.charCodeAt(0)) {
                    assert(null == inherit);
                    inherit = token;
                    break;
                }
                //name.split(' ').join('');
                if (!genPair(which, name, token)) {
                    return _error();
                }
                name = null;
                colon = false;
                break;
        }
        return true;
    }

    let token = null;
    let lastslash = false;
    while (null != (token = _getToken())) {
        if (token == '/') {
            if (lastslash) {
                _seekToEOL();
                lastslash = false;
                continue;
            }
            lastslash = true;
        }
        else {
            if (lastslash) {
                if (!_onToken('/'))
                    return null;
                lastslash = false;
            }
            if (!_onToken(token))
                return null;
            token = null;
        }
    }

    if (null != name || lastslash || cur != which[1][0] || 0 != which[0]) {
        _error();
        return null;
    }
    //console.log('end');
    return which;
}

function copySchema(to, from) {
    for (let x in from) {
        if ('__' != x.substring(0, 2)
            || '__requirelist__' == x
            || '__defaultlist__' == x
        )
            to[x] = from[x];
    }
}

function setupLink(sch, type) {
    let attr;
    if (!sch.hasOwnProperty('peer')) {
        attr = {__type__: 'PeterId', __up__: sch, __name__: 'peer', __element__: type};
        sch.peer = attr;
    }
    if (!sch.hasOwnProperty('name')) {
        attr = {__type__: 'String', __up__: sch, __name__: 'name'};
        sch.name = attr;
    }
    if (!sch.hasOwnProperty('time')) {
        attr = {__type__: 'Time', __up__: sch, __name__: 'time'};
        sch.time = attr;
    }
}

function installLink(cur, sch, peter, root, global) {
    let linkname = sch.__name__
        , peername
        , peerlink;

    if ('__container__' === sch.__element__) {
        let peersch = sch.peer;
        if (undefined == peersch) {
            console.log("Error: no peer defined in %s.%s container", peter.__name__, linkname);
            return false;
        }

        peername = ('PeterId' == peersch.__type__)
            ? peersch.__element__
            : peersch.__type__;
    }
    else {
        peername = sch.__element__;
        sch.__element__ = '__container__';
    }
    setupLink(sch, peername);

    assert('~' !== linkname);
    if (atCode == linkname.charCodeAt(1)) {
        if (linkname.substring(1) !== peername) {
            console.log("Error: peer %s conflicts with link name %s in %s", peername, peter.__name__, linkname);
            return false;
        }
        peerlink = '~' + peter.__name__;
    }
    else {
        peerlink = linkname;
    }

    // get peer's schema
    let refSchema;
    if (root.hasOwnProperty(peername)) {
        refSchema = root[peername];
    }
    else if (atCode == peername.charCodeAt(0) && null != global && global.hasOwnProperty(peername)) {
        refSchema = global[peername];
    }
    else {
        return false;
    }

    //console.log("finding %s.%s", refSchema.__name__, peerlink);
    if (refSchema.hasOwnProperty(peerlink)) {
        let sch2 = refSchema[peerlink];
        //console.log("%s %s %s", sch2.__type__, peter.__name__, sch2.__element__);
        if (sch2.hasOwnProperty('__to__') && sch2.__to__ == sch)
            return true;

        if ('__link__' == sch2.__type__) {
            if ('__container__' == sch2.__element__) {
                if (sch2.peer && (peter.__name__ == sch2.peer.__type__ || peter.__name__ == sch2.peer.__element__)) {
                    sch.__to__ = sch2;
                    sch2.__to__ = sch;
                    return true;
                }
            }
            else if (peter.__name__ == sch2.__element__) {
                sch.__to__ = sch2;
                sch2.__to__ = sch;
                return true;
            }
        }
    }
    console.log("Error: no pair link %s between %s %s", linkname, peter.__name__, peername);
    return false;
}

// first pass
// check set, list, link for its element type
// check other's type
function checkTypeAndInstallClass(sch, peter, root, global) {
    let type = sch.__type__;
    let inset = false;

    switch (type) {
        case '__list__':
        case '__set__':
        case '__keyed__':
            type = sch.__element__;
            if (isKeyword(type) || '__container__' == type)
                return true;
            inset = true;
            break;

        case '__link__':
        case '__container__':
            assert(false);
            return true;

        default:
            assert(!isKeyword(type));
            break;
    }

    let refSchema;
    if (root.hasOwnProperty(type)) {
        refSchema = root[type];
    }
    else if (/*atCode == type.charCodeAt(0) && */null != global && global.hasOwnProperty(type)) {
        refSchema = global[type];
    }
    else {
        return false;
    }

    if (type.charCodeAt(0) != atCode) {
        copySchema(sch, refSchema);

        // set reference and dependency
        if (!peter.hasOwnProperty('__ref__'))
            peter['__ref__'] = {};
        peter['__ref__'][type] = refSchema;

        if (!inset) {
            sch.__type__ = '__container__';
        }
        else {
            sch.__element__ = '__container__';
        }
    }
    else {
        // change type of Peter to PeterId
        if (!sch.hasOwnProperty('__element__')) {
            sch.__type__ = 'PeterId';
            sch.__element__ = type;
        }
        else {
            sch.__element__ = 'PeterId';
        }
    }
    return true;
}

function installSchema(which, global) {
    let untyped = which[2]
        , root = which[1][0]
        , inherits = which[3];
    for (let x in untyped) {
        let cur = untyped[x][0]
            , sch = untyped[x][1]
            , peter = untyped[x][2];

        //console.log("Check %s.%s", cur.__name__, sch.__name__);
        if ('__link__' === sch.__type__) {
            if (!installLink(cur, sch, peter, root, global)) {
                return false;
                //continue;
            }
        }
        else {
            if (!checkTypeAndInstallClass(sch, peter, root, global)) {
                function realType(sch) {
                    switch (sch.__type__) {
                        case '__link__':
                        case '__list__':
                        case '__set__':
                        case '__keyed__':
                            return sch.__element__;
                    }
                    return sch.__type__;
                }

                console.error("Error: fail to get schema of '%s' for %s.%s", realType(sch), cur.__name__, sch.__name__);
                return false;
                //continue;
            }
            // check key
            switch (sch.__type__) {
                case '__set__':
                    if (!(sch.__element__.charCodeAt(0) == atCode || isKeyword(sch.__element__))) {
                        console.error("Error: set can only contain simple type %s", sch.__name__);
                        return false;
                    }
                    break;

                case '__keyed__':
                    if (!sch.hasOwnProperty(sch.__key__)) {
                        console.error("Error: invalid key '%s' for map %s", sch.__key__, sch.__name__);
                        return false;
                    }
                    break;
            }
        }
    }

    // recursively add references
    function _addSchemaRefStr(sch, sset, check) {
        if (sset.hasOwnProperty('__ref__')) {
            if (sch != sset) {
                sch.__str__ = sset.__str__ + "\n" + sch.__str__;
                check[sset.__name__] = true;
            }

            for (let x in sset.__ref__) {
                if (!check[sset.__ref__[x].__name__]) {
                    _addSchemaRefStr(sch, sset.__ref__[x], check);
                }
            }
        }
        else if (sch != sset) {
            sch.__str__ = sset.__str__ + "\n" + sch.__str__;
            check[sset.__name__] = true;
        }
    }

    function _setupInheritance(sch, inherit) {
        let parent;
        if (root.hasOwnProperty(inherit)) {
            parent = root[inherit];
        }
        else if (null != global && global.hasOwnProperty(inherit)) {
            parent = global[inherit];
        }
        else {
            console.log("Can't find parent schema '%s' for '%s'", inherit, sch.__name__);
            return false;
        }

        sch.__inherit__ = parent;
        return true;
    }

    for (let x in root) {
        sch = root[x];
        //console.log("install %s", sch.__name__);
        if (sch.__type__ == '__peter__') {
            _addSchemaRefStr(sch, sch, {});
            //console.log(sch.__str__);

            if (inherits.hasOwnProperty(sch.__name__) && !_setupInheritance(sch, inherits[sch.__name__]))
                return false;
            // TODO: check consistency of parent & children attributes
        }
    }
    // TODO: check recursion
    return true;
}

//let checkForHexRegExp = new RegExp("^[0-9a-fA-F]{24}$");
function isValidPeterId(id) {
    if (null == id)
        return false;

    if ('string' === typeof id && id.length == 24) {
        for (let i=0; i<24; i++) {
            let c = id.charCodeAt(i);
            if (!(c>=digitCode0 && c<=digitCode9
              || c>=alphaCode_A && c<=alphaCode_Z
              || c>=alphaCode_a && c<=alphaCode_z))
                return false;
        }
        return true;
    }
    //if ('number' == typeof id && id.length == 12)
    //  return true;
    return false;
}

function checkSimpleType(value, type) {
    switch (type) {
        case 'Integer':
        case 'Number':
            return 'number' === typeof value;

        case 'String':
            return 'string' === typeof value;

        case 'BOOL':
            return 'boolean' === typeof value;

        case 'Object':
            return 'object' === typeof value;

        case 'Time':
            return value instanceof Date;

        case 'PeterId':
        case 'PeterID':
            return value instanceof ObjectID;

        default:
            console.log("Unknown type", type, 'of value', value);
            assert(false);
    }
    return false;
}

function checkSimpleTypeAndReplcaceValue(ind, type, parentJson) {
    let value = parentJson[ind];
    switch (type) {
        case 'Integer':
        case 'Number':
            if ('number' === typeof value) {
                return true;
            }
            if ('string' === typeof value) {
                let n = parseFloat(value);
                if (!isNaN(n)) {
                    parentJson[ind] = n;
                    return true;
                }
            }
            return false;

        case 'String':
            return 'string' === typeof value;

        case 'BOOL':
            if ('boolean' === typeof value)
                return true;
            if ('string' === typeof value) {
                let lower = value.toLowerCase();
                if ('true'==lower || 'false'==lower) {
                    parentJson[ind] = ('true'==lower);
                    return true;
                }
            }
            return false;

        case 'Object':
            return 'object' === typeof value;

        case 'Time':
            if (value instanceof Date)
                return true;
            if ('number' === typeof value || 'string' === typeof value) {
                parentJson[ind] = new Date(value);
                return true;
            }
            return false;

        case 'PeterId':
        case 'PeterID':
            if (value instanceof ObjectID) {
                return true;
            }
            if (isValidPeterId(value)) {
                parentJson[ind] = new ObjectID(value);
                return true;
            }
            return false;

        default:
            assert(false);
    }
    return false;
}

function checkTypeAndReplcaceValue(name, sch, parentJson) {
    if (!sch)
        return false;

    let value = parentJson[name];
    switch (sch.__type__) {
        case 'Integer':
        case 'String':
        case 'BOOL':
        case 'Time':
        case 'PeterId':
        case 'PeterID':
            return checkSimpleTypeAndReplcaceValue(name, sch.__type__, parentJson);

        default:
            return check(sch, value);
    }
    return false;
}

function _findAttribute(sch, name) {
    //console.log("finding %s in %s", name, sch.__name__);
    if (sch.hasOwnProperty(name)) {
        return sch[name];
    }
    if (sch.hasOwnProperty('__inherit__')) {
        return findAttribute(sch.__inherit__, name);
    }
    return null;
}

function findAttribute(sch, name) {
    let arr = name.split('.');
    let attr = sch;
    for (let x in arr) {
        attr = _findAttribute(attr, arr[x]);
        if (!attr)
            return null;
    }
    return attr;
}

function findAttribute2(sch, name) {
    // try to add []
    if (name.substr(0, 1) != '[') {
        let name2 = '[' + name + ']';
        if (sch.hasOwnProperty(name2)) {
            return sch[name2];
        }
        if (sch.hasOwnProperty('__inherit__')) {
            return findAttribute(sch.__inherit__, name2);
        }
    }
    return null;
}

function checkRequired(sch, json) {
    let list = sch.__requirelist__;
    if (undefined != list) {
        for (let i in list) {
            if (!json.hasOwnProperty(list[i])) {
                // try to add []
                let name = list[i];
                if (name.substr(0, 1) == '[') {
                    let name2 = name.substr(1, name.length - 2);
                    if (json.hasOwnProperty(name2)) {
                        json[name] = json[name2];
                        delete json[name2];
                        return true;
                    }
                }
                console.log("Error: required attribute %s.%s is not provided", sch.__name__, name);
                return false;
            }
        }
    }

    if (sch.hasOwnProperty('__inherit__')) {
        return checkRequired(sch.__inherit__, json);
    }
    return true;
}

function checkOne(sch, json, type, required) {
    assert(sch, "null schema is provided");
    assert(type, "null type is provided");
    if (undefined == required)
        required = true;

    //console.log("checkOne %s %s %s", sch.__name__, json, type);
    switch (type) {
        case '__container__':
        case '__peter__':
            // check whether required attributes are provided
            if (required && !checkRequired(sch, json))
                return false;

            // check if key is provided
            if ('__keyed__' == sch.__type__) {
                if (!json.hasOwnProperty(sch.__key__)) {
                    console.log("Error: key must be provide for map '%s'", sch.__name__);
                    return false;
                }
            }

            // check each attributes in the element
            //console.log("Check %s", JSON.stringify(json));
            //console.log("%s {", sch.__name__);
            let attr;
            for (let x in json) {
                if (undefined == json[x]) {
                    delete json[x];
                    continue;
                }
                if (x == '__zip__')
                    continue;

                attr = findAttribute(sch, x);
                if (null == attr) {
                    // try to add []
                    attr = findAttribute2(sch, x);
                    if (null == attr) {
                        console.log("Error: no schema for %s.%s", sch.__name__, x);
                        if ('_id' == x) {
                            console.log('\tDon\'t specify _id for a new peter object');
                        }
                        return false;
                    }
                    json[attr.__name__] = json[x];
                    delete json[x];
                    x = attr.__name__;
                }

                if ('Object' == attr.__type__) {
                    continue;
                }
                //console.log("check %s %s", x, JSON.stringify(json[x]));
                if (!checkTypeAndReplcaceValue(x, attr, json)) {
                    //console.log("%s %s", typeof json[x], sch.__type__);
                    console.log("Error: invalid value '%s' for %s.%s", json[x].toString().substring(0, 360), sch.__name__, x);
                    return false;
                }
            }
            //console.log("}");
            break;

        default:
            if (undefined != json && !checkSimpleType(json, type)) {
                console.log("Error: invalid value '%s' for %s", json, sch.__name__);
                return false;
            }
    }

    return true;
}

function check(sch, json, required) {
    assert(sch, "null schema is provided");

    let value;
    switch (sch.__type__) {
        case '__list__':
        case '__set__':
        case '__keyed__':
        case '__link__':
            //console.log("Check each in array %s [", name);
            if (!(json instanceof Array)) {
                console.log("Warning: not an array for %s", sch.__name__);
                return false;
            }

            switch (sch.__element__) {
                case '__container__':
                    for (let x in json) {
                        value = json[x];
                        if (!checkOne(sch, value, sch.__element__, required)) {
                            return false;
                        }
                    }
                    break;

                case '__peter__':
                    assert(false);
                    break;

                case 'Object':
                    break;

                default:
                    for (let x in json) {
                        if (undefined != json[x]) {
                            if (!checkSimpleTypeAndReplcaceValue(x, sch.__element__, json)) {
                                console.log("Error: invalid value '%s' in %s.%s", json[x], sch.__name__, x);
                                return false;
                            }
                        }
                        else {
                            delete json[x];
                        }
                    }
                    break;
            }
            //console.log("]");
            break;

        default:
            return checkOne(sch, json, sch.__type__, required);
    }
    return true;
}

let BasicSchemas;
function getBasicTypeSchema(type) {
    if (BasicSchemas) {
        return BasicSchemas[type];
    }

    const types = ['String', 'Integer', 'Number', 'BOOL', 'Time', 'PeterID', 'PeterId', 'Double', 'Object'];
    BasicSchemas = {};
    for (let x in types) {
        BasicSchemas[types[x]] = {__name__: types[x], __type: types[x]};
    }
    return BasicSchemas[type];
}

function parse(data, global) {
    try {
        let which = parseData(data);

        if (which != null && installSchema(which, global)) {
            return which[1][0];
        }
    }
    catch (e) {
        console.log(e.stack);
    }
    return null;
}

module.exports = {
    parse: parse,

    print: function (schema) {
        return printSchema(schema, 0);
    },

    checkCompatibility: function (oldSchema, newSchema) {
        // TODO::
        return true;
    },

    findAttribute: findAttribute,

    check: check,

    checkElement: function (sch, json) {
        assert(sch);
        switch (sch.__type__) {
            case '__list__':
            case '__set__':
            case '__keyed__':
            case '__link__':
                return checkOne(sch, json, sch.__element__, false);

            default:
                return check(sch, json, false);
        }
    },

    isValidPeterId: isValidPeterId,

    getBasicTypeSchema: getBasicTypeSchema
};
