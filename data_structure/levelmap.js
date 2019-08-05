function levelmap() {
    this.map = {'__': true};
}

levelmap.prototype = {
    addl2: function (keyl1, keyl2, value) {
        if (keyl1=='' || keyl2=='') {
            return;
        }
        let l1 = this.map[keyl1];
        if (!l1) {
            l1 = {'__': true};
            this.map[keyl1] = l1;
        }
        let l2 = l1[keyl2];
        if (!l2) {
            l2 = [];
            l1[keyl2] = l2;
        }
        l2.push(value);
    },

    addl1: function (key, value) {
        if (key == '') {
            return;
        }
        let l1 = this.map[key];
        if (!l1) {
            l1 = [];
            this.map[key] = l1;
        }
        l1.push(value);
    },

    toArray: function() {
        function _to(map) {
            if (!map.hasOwnProperty('__')) {
                return map;
            }
            let out = [];
            for (let x in map) {
                if (x != '__') {
                    out.push([x, _to(map[x])]);
                }
            }
            return out;
        }
        return _to(this.map);
    }
};

module.exports = levelmap;
