##  $save 0
##

::  _bindDb dbUrl ?options
=>  {{
    var self = this;
    if (self.db) {
        $callback 'Already bound' null
    }
    var opts = utils.copyObj({useNewUrlParser: true}, options);
    var client = new MongoClient(dbUrl, opts);
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
    var names = peter.sm.getAllSchemaName();
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
    var err = $?;

    $return MongoOP.removeMap _getCollection(self, pid1) pid1 link1 'peer' pid2
    <=  err 0
    <-  err 0
    ;;
}}
;;
