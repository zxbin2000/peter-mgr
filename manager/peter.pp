##  $save 0
##

::  _bindDb dbUrl ?options
=>  {{
    let self = this;
    if (self.db) {
        $callback null 'Already bound'
    }
    let opts = options || {};
    opts.useNewUrlParser = true;
    let client = new MongoClient(dbUrl, opts);
}}
    client.connect
=>  {{
    self.db = $@.db();
}}
    self.sm.loadFromDB self.db
;;


::  _drop coll db
=>  {{
    console.log('To drop', coll);
}}
    db.collection(coll).drop
;;


::  _procClear peter
=>  {{
    let names = peter.sm.getAllSchemaName();
    names.push('_schema');
}}
    ! names _drop $= peter.db
;;


::  doLink self pid1 pid2 link1 link2 att1 att2
=>  MongoOP.pushMap _getCollection(self, pid1) pid1 link1 'peer' att1 false
=>  MongoOP.pushMap _getCollection(self, pid2) pid2 link2 'peer' att2 false
<-  $? 0
<=  null 1
->  {{
    let err = $?;

    $return MongoOP.removeMap _getCollection(self, pid1) pid1 link1 'peer' pid2
    <=  err 0
    <-  err 0
    ;;
}}
;;
