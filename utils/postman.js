let request = require('request');
let crypto = require('crypto');
let utils = require('./utils');
let VERSION = '1.1.0';


if (typeof String.prototype.startsWith != 'function') {
    String.prototype.startsWith = function (str){
        return this.slice(0, str.length) == str;
    };
}

module.exports = function (addr, options) {
    let apiKey = ''
      , apiSecret = ''
      , mute = false
      , timeout = 0;
    if (addr.startsWith('http://') || addr.startsWith('https://')) {
        if (options) {
            if (options.apiKey)
                apiKey = options.apiKey;
            if (options.apiSecret)
                apiSecret = options.apiSecret;
            if (options.mute)
                mute = options.mute;
            if (options.timeout)
                timeout = options.timeout;
        }
        return new Postman(apiKey, apiSecret, utils.removeTailIf(addr, '/'), mute, timeout);
    }

    // for back-compatibility
    if (arguments.length < 3) {
        throw new Error('Bad arguments for Postman');
    }
    apiKey = arguments[0];
    apiSecret = arguments[1];
    addr = arguments[2];
    mute = arguments[3] ? true : false;
    return new Postman(apiKey, apiSecret, utils.removeTailIf(addr, '/'), mute, timeout);
};

function Postman(apiKey, apiSecret, apiUrl, mute, timeout) {
    this.apiKey = apiKey;
    this.apiSecret = apiSecret;
    this.apiURL = apiUrl;
    this.mute = mute;
    this.timeout = timeout;
}

Postman.prototype.get = function (path, options, callback) {
    this._request('get', path, {}, options, callback);
};

Postman.prototype.post = function (path, data, options, callback) {
    this._request('post', path, data, options, callback);
};

Postman.prototype.put = function (path, data, options, callback) {
    this._request('put', path, data, options, callback);
};

Postman.prototype.del = function (path, data, options, callback) {
    this._request('del', path, data, options, callback);
};

Postman.prototype._request = function(type, path, params, options, callback) {
    let cookie;
    if ('function' == typeof options) {
        callback = options;
        options = {};
    }
    else if (!options) {
        callback = params;
        params = {};
        options = {};
    }
    else if (options.cookie) {
        cookie = options.cookie;
    }
    let timeout = options.timeout || this.timeout;

    let ms = String(new Date().getTime());
    let user_agent = 'Postman/'+VERSION+' (node '+process.version+')';
    let headers = {
        'User-Agent': user_agent,
        'X-Authentication-Key': this.apiKey,
        'X-Authentication-Nonce': ms,
        'X-Authentication-Signature': crypto.createHmac('SHA256', this.apiSecret).update(ms).digest('base64')
    };

    let j = request.jar();
    let arg = {
        url: this.apiURL + ('/'==path.substr(0, 1) ? path : '/' + path),
        encoding: null,
        headers: headers,
        qs: {},
        jar: j,
        strictSSL: true
    };
    if (!options.nojson) {
        arg.json = true;
        arg.body = params;
    }
    else {
        arg.form = params;
    }

    let mute = this.mute;
    let self = this;
    let callback_check = function (err, res, body) {
        //console.log('callback_check', body);
        if (!callback) {
            return;
        }
        if (!mute) {
            console.log(res ? res.statusCode : 400, arg.url);
        }
        if (!err) {
            if (String(res.statusCode).startsWith('20')) {
                self.headers = res.headers;
                callback(null, body);
            }
            else {
                if (body instanceof Buffer) {
                    body = body.toString();
                }
                callback(body, res ? res.statusCode : 400);
            }
        }
        else {
            callback(err, res ? res.statusCode : 400);
        }
        callback = undefined;
    };

    //if (cookie) {
    //    let cookie = request.cookie(cookie);
    //    j.setCookie(cookie, arg.url);
    //}

    switch(type) {
        case 'post':
            request.post(arg, callback_check);
            break;

        case 'get':
            request.get(arg, callback_check);
            break;

        case 'put':
            request.put(arg, callback_check);
            break;

        case 'del':
            request.del(arg, callback_check);
            break;
    }
    if (timeout) {
        setTimeout(function () {
            if (callback) {
                if (!mute) {
                    console.log(utils.now(), 'Timedout', arg.url);
                }
                callback('Timeout', 408);
                callback = undefined;
            }
        }, utils.translateIntervalString(timeout));
    }
};
