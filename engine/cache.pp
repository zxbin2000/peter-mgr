::  expire self key cache
=>  cache[0].lock
=>  {{
    if (cache[2]) {
        cache[1] = null;
        cache[2] = null;
        if (self.verbose) {
            console.log('$'+self.name+'$.'+key, 'expired');
        }
    }
    assert(!cache[1]);
}}
    cache[0].unlock
;;


{{
    function extendTimer(self, key, cache) {
        if (self.ttl) {
            if (cache[2])
                clearTimeout(cache[2]);
            cache[2] = setTimeout(expire, self.ttl, self, key, cache, function () {});
        }
    }
}}


::  load self key loader ?lock
=>  {{
    var cache = self.all[key];
    if (!cache) {
        cache = [Lock.create(key), null, null];
        self.all[key] = cache;
    }

    var old_callback = callback;
    callback = function (err, arg) {
        if (!lock) {
            if (null == err) {
                extendTimer(self, key, cache);
            }
            cache[0].unlock();
        }
        old_callback(err, cache);
    }
}}
    cache[0].lock
=>  {{
    if (cache[1]) {
        $callback null 0
    }
}}
    loader key
<=  {{
    cache[1] = $@;
    if (self.verbose) {
        console.log('$'+self.name+'$.'+key, 'loaded', cache[1]);
    }
}}
    null 0
<-  $? 0
;;


::  read self key ?loader
=>  load self key (loader || self.loader)
<=  null $@[1]
<-  $? 0
;;


::  read_change self key ?options
=>  {{
    if (!options) {
        options = {loader: self.loader};
    }
    else if (!options.loader) {
        options.loader = self.loader;
    }

    var cache;
    var old_callback = callback;
    callback = function (err, arg) {
        if (null == err) {
            if (self.verbose) {
                console.log('$'+self.name+'$.'+key, 'changed', arg);
            }
            cache[1] = arg;
            extendTimer(self, key, cache);
            if (arguments.length > 2) {
                arg = arguments[2];
            }
        }
        cache[0].unlock();
        old_callback(err, arg);
    }
}}
    load self key options.loader true
=>  {{
    cache = $@;
    if (!options.on_succ) {
        $callback null cache[1]
    }
}}
    options.on_succ key cache[1]
<-  {{
    cache = $@;
    if (options.on_fail) {
        $return options.on_fail key $?
        ;;
    }
}}
    $? 0
;;
