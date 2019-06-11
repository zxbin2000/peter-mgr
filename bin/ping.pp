{{
    var URL = require('url');
    var n = 1;
}}


::  ping url
=>  {{
    var time = process.hrtime();
    var o = URL.parse(url);
    var nc = Postman(o.protocol+'//'+ o.host, {mute: true, timeout: 10000});
}}
    nc.get o.path {nojson: true}
=>  {{
    console.log('#%d: %s, %s', n, utils.printSize($@.length), utils.elapsed(time));
    setTimeout(ping, 1000, url, function () {});
    n ++;
}}
->  {{
    console.log('#%d: %d, %s', n, $@, $?);
    setTimeout(ping, 1000, url, function () {});
    n ++;
}}
;;


::  main argv
=>  {{
    if (argv.length < 2) {
        console.log('Usage: ping url');
        $callback null 0
    }
    console.log('Ping', argv[1]);
}}
    ping argv[1]
;;

