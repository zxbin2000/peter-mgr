const config = require('config');
const peter = require('../index').getManager();
const assert = require('assert');
const should = require('should');

const user = { avatar: 'default', gender: 1, real_name: 'test-peter' };

describe('Peter', function() {
  before(function() {
    return peter.bindDbAsync(config.get('schema')).then(args => {
      let tasks = [];
      for(let i = 0; i < 10; i++) {
        let user = {
          avatar: 'test-peter-user-avatar-' + i,
          gender: i % 2,
          real_name: 'test-peter-user-' + i,
          is_deleted: false
        };
        tasks.push(peter.createAsync('@User', user));
      }
      return Promise.all(tasks);
    }).then(args => {
      should.equal(args.length, 10);
    }).catch(error => {
      console.log('TestError: ', error);
      should.null(error);
    });
  });

  beforeEach(function() {
    delete user._id;
    delete user._schemaid;
  });

  after(function() {
    return peter.findAsync('@User', { is_deleted: false }).then(args => {
      return Promise.all(args.map(item => {
        return peter.destroyAsync(item._id);
      }));
    }).then(args => {
      should(args.length).be.aboveOrEqual(10);
      process.exit(0);
    }).catch(error => {
      console.log('TestError: ', error);
    });
  });

  describe('#create()', function() {
    it('单测 create 方法', function() {
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
    it('单测 createS 方法', function() {
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
    it('单测 find 方法', function() {
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
    it('单测 findOne 方法', function() {
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
    it('单测 get 方法', function() {
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

  describe('#set()', () => {
    it('单测 set 方法', () => {
      let user_id;
      return peter.findOneAsync('@User', { is_deleted: false }).then(args => {
        should.exist(args._id);
        user_id = args._id;
        return peter.setAsync(args._id, { details: 'test-set-detail' });
      }).then(args => {
        should.equal(args.n, 1);
        should.equal(args.ok, 1);
        return peter.getAsync(user_id);
      }).then(args => {
        should.exist(args);
        should.exist(args.details);
        return peter.createAsync('@UserSession', { openid: 'test-openid' });
      }).then(args => {
        console.log('args=', args, user_id);
        return peter.setAsync(args, { user_id: user_id });
      }).then(args => {
        should.equal(args.n, 1);
        should.equal(args.ok, 1);
      }).catch(error => {
        console.log('TestError: ', error);
        should.not.exist(error);
      });
    });
  });

  describe('#remove()', function() {
    it('单测 remove 方法', function() {
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
    it.only('单测 findOneAndUpdate 方法', function() {
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
        return peter.findOneAndUpdateAsync('@User', {
          real_name: user.real_name
        }, { $set: nval }, { upsert: true });
      }).then(args => {
        assert.equal(args._id.toString().length, 24);
        assert.equal(args.real_name, nval.real_name);
        return peter.findOneAndUpdateAsync('@User', {
          real_name: nval.real_name
        }, { profile: 'test-profile' });
      }).then(args => {
        assert.equal(args._id.toString().length, 24);
      }).catch(error => {
        console.log('Error: ', error);
        assert(error === null);
      });
    });
  });

  describe('#findOneAndDelete()', function() {
    it('单测 findOneAndDelete 方法', () => {
      return peter.createAsync('@User', user).then(args => {
        return peter.findOneAndDeleteAsync('@User', { real_name: user.real_name });
      }).then(args => {
        assert.equal(args._id.toString(), user._id);
      }).catch(error => {
        console.log('Error: ', error);
      });
    });
  });

  describe('#findOneAndReplace()', () => {
    it('测试 Schema 检查是否启用', () => {
      return peter.createAsync('@User', user).then(args => {
        return peter.findOneAndReplaceAsync('@User', { 
          real_name: user.real_name 
        }, {
          avatar: 'avatar-1',
          gender: 1,
          not_exist: "not exist"
        });
      }).catch(error => {
        should.equal(error.name, 'Error');
        should.equal(error.message, 'Schema check error: @User');
        peter.destroyAsync(user._id);
      });
    });

    it('测试 Replace 是否成功', () => {
      let real_name = 'test-find-replace';
      let avatar = 'test-avatart-2';
      return peter.createAsync('@User', user).then(args => {
        return peter.findOneAndReplaceAsync('@User', {
          real_name: user.real_name
        }, {
          avatar: avatar,
          gender: 1,
          real_name: real_name
        });
      }).then(args => {
        should.exist(args._schemaid);
        should.exist(args.create_time);
        should.equal(args.real_name, real_name);
        should.equal(args.avatar, avatar);
        return peter.destroyAsync(args._id);
      }).then(args => {
        should.equal(args.n, 1);
        should.equal(args.ok, 1);
      }).catch(error => {
        console.log('TestError: ', error);
        should.null(error);
      });
    });
  });

  describe('#count()', () => {
    it('单测 count 方法', () => {
      return peter.countAsync('@User', {}).then(args => {
        args.should.be.above(0);
      }).catch(error => {
        console.log('TestError: ', error);
        should.null(error);
      });
    });
  });

  describe('#distinct()', () => {
    it('单测 distinct 方法', () => {
      return peter.distinctAsync('@User', 'real_name').then(args => {
        should(args.length).be.aboveOrEqual(10);
      }).catch(error => {
        console.log('TestError: ', error);
        should.null(error);
      });
    });
  });

});
