##  $save 0
##

{{
    function array2Map(array, key, value) {
        let map = {};
        for (let i = 0; i < array.length; i++) {
            map[array[i][key]] = array[i][value];
        }
        return map;
    }

    let peter;
    let groups = {};
}}


::  init pm
=>  {{
    peter = pm;
}}
    peter.get '@System::Index.1' 'groups'
<=  {{
    if($@ === null) {
      $callback null groups
    }
    groups = array2Map($@, 'name', 'id');
}}
    null groups
->  {{
    switch ($?) {
        case 'No such fields':
            $callback null groups

        case 'Invalid pid \'@System::Index.1\' provided':
            let fs = require('fs');
            // TODO::: fix file path
            let data = fs.readFileSync(utils.replaceStringTail(__filename, 'js', 'ps'));

            $return peter.sm.update data
            =>  peter.sm.setAutoId '@System::Index' 0
            =>  init peter
            ;;
    }

    $return peter.sm.setAutoId '@System::Index' 0
    =>  peter.createS '@System::Index' {}
    <=  null groups
    ;;
}}
;;


::  create groupname ?groupdesc
**  NOTNULL groupname
=>  {{
    if (undefined == groupdesc) {
        groupdesc = null;
    }
    if (groups.hasOwnProperty(groupname)) {
        $callback null groups[groupname]
    }
    let json = (null == groupdesc)
             ? {name: groupname}
             : {name: groupname, desc: groupdesc};
}}
    peter.createS '@System::Index' json
=>  {{
    let id = $@;
    json = (null == groupdesc)
         ? {name: groupname, id: id}
         : {name: groupname, desc: groupdesc, id: id};
}}
    peter.push '@System::Index.1' '[groups]' json
<=  {{
    groups[groupname] = id;
}}
    null id
->  {{
    // should have already been created
    $return peter.getElementByKey '@System::Index.1' '[groups]' groupname
    <=  {{
        id = $@.id;
        groups[groupname] = id;
    }}
        null id
    ;;
}}
;;


::  save2 groupid json
=>  peter.push '@System::Index.'+groupid '[values]' json
<=  null $@
->  {{
    $return peter.replaceElementByKey '@System::Index.'+groupid '[values]' json
    ;;
}}
;;


::  save groupname key value
=>  {{
    if (!groups.hasOwnProperty(groupname)) {
        $return create groupname 'desc'
        =>  save2 groups[groupname] {key: key, value: value}
        <-  {{
            if ('Already existed' == $?) {
                assert(groups.hasOwnProperty(groupname));

                $return save2 groups[groupname] {key: key, value: value}
                ;;
            }
        }}
            null $@
        ;;
    }
}}
    save2 groups[groupname] {key: key, value: value}
;;


::  load ?groupname ?key
=>  {{
    if (undefined==groupname || null==groupname) {
        $callback null groups
    }
    if (undefined==key || null==key) {
        if (!groups.hasOwnProperty(groupname)) {
            $callback 'Not existing' null
        }

        $return peter.get '@System::Index.'+groups[groupname] '[values]'
        <=  null array2Map($@, 'key', 'value')
        <-  {{
            if ('No such fields' == $?) {
                $callback null {}
            }
        }}
            $? $@
        ;;
    }
}}
    peter.getElementByKey '@System::Index.'+groups[groupname] '[values]' key
<=  null array2Map([$@], 'key', 'value')
;;

