##  $save 0
##

::  _loadFromDB db
=>  {{
    var str = ''
      , map = {}
      , self = this;

    self.collection = db.collection('_schema');
}}
    MongoOP.get self.collection 0 'schema'
=>  {{
    function readEachSchema(each, callback) {
        assert(each.id);

        $return MongoOP.get self.collection each.id null
        <=  {{
            str += $@.str;
            map[each.name] = $@;
        }}
            null 0
        ;;
    }
}}
    ! $@ readEachSchema $=
<-  (($? == 'Not existing') ? null : $?) 0
<=  {{
    var schema = Parser.parse(new Buffer(str));
    assert(schema);

    var sch, each
      , num = 0;
    for (var x in schema) {
        sch = schema[x];
        if (sch.__type__ == '__peter__') {
            each = map[sch.__name__];
            sch.__key__ = each.key;
            sch.__id__ = each.hasOwnProperty('_id') ? each._id : each.id;
            sch.__time__ = each.time;
            sch.__who__ = each.who;
            addSchema(self, sch, true);
            num++;
        }
        else {
            addSchema(self, sch, false);
        }
    }
}}
    null num
;;
