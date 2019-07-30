const config = require('config');
const peter = require('../index').getManager();
const assert = require('assert');

const user = { avatar: 'default', gender: 1, real_name: 'test-peter' };

describe('Peter', function() {
  before(function() {
    return peter.bindDbAsync(config.get('schema'));
  });

  beforeEach(function() {
    delete user._id;
    delete user._schemaid;
  });

  after(function() {
    process.exit(0);
  });

  describe('#create()', function() {
    it('should return peter id', function() {
      return peter.createAsync('@User', user).then(function(args) {
        assert.equal(args.toString().length, 24);
        return args;
      }).then(function(args) {
        return peter.destroyAsync(args);
      }).then(function(args) {
        assert.equal(args.n, 1);
        assert.equal(args.ok, 1);
      }).catch(function(error) {
        assert.equal(error, null);
      });
    });
  });

  describe('#createS()', function() {
    it('should return sequence number', function() {
      return peter.createSAsync('@User', user).then(function(args) {
        assert(parseInt(args.toString()) > 0);
        return args;
      }).then(function(args) {
        return peter.destroyAsync('@User.' + args);
      }).then(function(args) {
        assert.equal(args.n, 1);
        assert.equal(args.ok, 1);
      });
    });
  });

  describe('#find()', function() {
    it('should return an object', function() {
      let user_id;
      return peter.createAsync('@User', user).then(function(args) {
        assert.equal(args.toString().length, 24);
        user_id = args.toString();
        return user_id;
      }).then(function(args) {
        return peter.findOneAsync('@User', { 
          real_name: user.real_name 
        }, { sort: { create_time: -1 } });
      }).then(function(args) {
        assert.equal(user_id, args._id.toString());
        assert.equal(args.avatar, user.avatar);
        assert.equal(args.real_name, user.real_name);
        return args;
      }).then(function(args) {
        let pid = user_id;
        return peter.destroyAsync(pid);
      }).then(function(args) {
        assert.equal(args.n, 1);
        assert.equal(args.ok, 1);
      }).catch(function(error) {
        console.log('Error: ', error);
      });
    });
  });

  describe('#findOne()', function() {
    it('should return only one', function() {
      let tasks = [
        { avatar: 'test', gender: 0, real_name: 'test-find' },
        { avatar: 'test', gender: 0, real_name: 'test-find' },
        { avatar: 'test', gender: 0, real_name: 'test-find' },
        { avatar: 'test', gender: 0, real_name: 'test-find' }
      ];
      return Promise.all(tasks.map(item => {
        return peter.createAsync('@User', item);
      })).then(args => {
        assert.equal(args.length, 4);
        return peter.findOneAsync('@User', { real_name: 'test-find' });
      }).then(args => {
        assert.equal(typeof args, 'object');
        assert.equal(args.real_name, 'test-find');
        return peter.findAsync('@User', { real_name: 'test-find' });
      }).then(args => {
        assert.equal(args.length, 4);
        return Promise.all(args.map(item => {
          return peter.destroyAsync(item._id);
        }));
      }).then(args => {
        assert.equal(args.length, 4);
      }).catch(error => {
        console.log('Error: ', error);
      });

    });
  });

  describe('#get()', function() {
    it('should get prop', function() {
      return peter.createAsync('@User', user).then(args => {
        assert.equal(args.toString().length, 24);
        return peter.getAsync(args);
      }).then(args => {
        assert.equal(args.avatar, user.avatar);
        assert.equal(args.gender, user.gender);
        assert.equal(args.real_name, user.real_name);
        return peter.destroyAsync(args._id);
      }).then(args => {
        assert.equal(args.n, 1);
        assert.equal(args.ok, 1);
      }).catch(error => {
        console.log('Error: ', error);
      });
    });
  });

  describe('#remove()', function() {
    it('should remove prop', function() {
      return peter.createAsync('@User', user).then(args => {
        return peter.removeAsync(args, 'real_name');
      }).then(args => {
        assert.equal(args.n, 1);
        assert.equal(args.ok, 1);
        return peter.getAsync(user._id);
      }).then(args => {
        assert.equal(args.real_name, null);
        assert.equal(args._id.toString(), user._id.toString());
        return peter.destroyAsync(user._id);
      }).then(args => {
        assert.equal(args.n, 1);
        assert.equal(args.ok, 1);
      }).catch(error => {
        console.log('Error: ', error);
      });
    });
  });

  describe('#findOneAndUpdate()', function() {
    it.only('should return only one', function() {
      let nval = { avatar: 'default', gender: "1", real_name: 'test-find-update' };
      return peter.findOneAndUpdateAsync('@User', { 
        real_name: user.real_name
      }, nval, { upsert: true }).then(args => {
        assert.equal(args._id.toString().length, 24);
        assert.equal(args.real_name, nval.real_name);
        return peter.getAsync(args._id);
      }).then(args => {
        assert.equal(args._id.toString().length, 24);
        assert.equal(args.real_name, nval.real_name);
        return peter.destroyAsync(args._id);
      }).then(args => {
        assert.equal(args.n, 1);
        assert.equal(args.ok, 1);
      }).catch(error => {
        console.log('Error: ', error);
      });
    });
  });

});
