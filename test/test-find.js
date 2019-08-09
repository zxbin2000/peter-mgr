const config = require('config');
const peter = require('../index').getManager();
const assert = require('assert');
const should = require('should');

describe('查询方法单元测试', () => {

  before(() => {
    return peter.bindDbAsync(config.get('schema'));
  });

  it('#findOne', () => {
    return peter.findOneAsync('@Empty').then(args => {
      assert.equal(args, null);
    }).catch(error => {
      console.log(error);
    });
  });

  it('#get', () => {
    return peter.getAsync(1).then(args => {
      console.log(args);
    }).catch(error => {
      assert.equal(error.name, 'Error');
      assert.equal(error.message, "Invalid pid '1' provided");
    });
  });

});
