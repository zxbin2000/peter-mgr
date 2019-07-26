#!/usr/bin/env node

let peter, index, utils;
try {
    peter = require('./manager/peter').createManager();
    index = require('./index/index');
    utils = require('./utils/utils');
} catch (e) {
    let _peter = require('peter');
    peter = _peter.createManager();
    index = _peter.Index;
    utils = _peter.Utils;
}
let fs = require('fs');
let beautify = require('js-beautify').js_beautify;

let mongoaddr = utils.mongoAddrFromConf();;
let argv = utils.getRealArgv(__filename);

if (argv.length < 2) {
    console.log('get pid [-np] [-o [out.json]]');
    process.exit(-1);
}

let out, pid, bPrint = true;
pid = argv[1];

for (let i=2; i < argv.length; i++) {
    switch (argv[i]) {
        case '-np':
            bPrint = false;
            break;
        case '-o':
            if (i < argv.length - 1) {
                out = argv[++i];
            }
            else {
                out = pid + '.json';
            }
            break;
    }
}
console.log('get %s', pid, out ? '-> ' + out : '');

peter.bindDb(mongoaddr, (error, args) => {
  if(error) {
    console.log('Error: ', error);
    process.exit(-1);
  }
  peter.get(pid, (error, args) => {
    if(error) {
      console.log('Error: ', error);
      process.exit(-1);
    }
    let str = beautify(JSON.stringify(arg));
    if (bPrint) {
        console.log(str);
    }
    if (undefined != out) {
        fs.writeFile(out, str, cont);
    }
    process.exit(0);
  });
});
