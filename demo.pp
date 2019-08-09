##  $conf { port: 8000 }
##  $verbose ['url']
##  $save 0

{{
    const config = require('config');
    const peter = require('../index').getManager();
    const db_url = config.get('schema');
}}

::  main
=>  peter.bindDb db_url
<=  null null
;;

::  /test foo
=>  {{
    console.log('====', foo);
}}
    peter.findOne '@User'
<=  null $@
<-  $?
;;