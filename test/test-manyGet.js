const _ = require('lodash');
const config = require('config');
const peter = require('../index').getManager();
const assert = require('assert');
const should = require('should');

describe('manyGet 方法单元测试', () => {

  before(() => {
    return peter.bindDbAsync(config.get('schema')).then(args => {
      return Promise.all([
        peter.createAsync('@User', { avatar: 'test-manyGet', gender: 0, nick_name: 'test' }),
        peter.createAsync('@User', { avatar: 'test-manyGet', gender: 0, nick_name: 'test' }),
        peter.createAsync('@User', { avatar: 'test-manyGet', gender: 0, nick_name: 'test' })
      ]);
    });
  });

  it('#manyGet', () => {
    let uids;
    return peter.findAsync('@User').then(args => {
      let uids = _.map(args, item => {
        return item._id;
      });
      return uids;
    }).then(args => {
      uids = args;
      return peter.manyGetAsync(uids, ['avatar']);
    }).then(args => {
      for(let key in args) {
        assert(typeof args[key] == 'object');
        assert(args[key]['_id'] == key);
      }
      return peter.manyGetAsync(uids, 'avatar');
    }).then(args => {
      for(let key in args) {
        assert(typeof args[key] == 'string');
        assert(args[key]['_id'] == undefined);
      }
    });
  });

});
