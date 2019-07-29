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
    it.only('should return an object', function() {
      let user_id;
      return peter.createAsync('@User', user).then(function(args) {
        assert.equal(args.toString().length, 24);
        user_id = args.toString();
        return user_id;
      }).then(function(args) {
        return peter.findOneAsync('@User', { 
          real_name: user.real_name 
        }, { limit: 1, sort: { create_time: -1 } });
      }).then(function(args) {
        assert.equal(user_id, args._id.toString());
        assert.equal(args.avatar, user.avatar);
        assert.equal(args.real_name, user.real_name);
        return args;
      }).then(function(args) {
        let pid = user_id;
        console.log(typeof pid, pid);
        return peter.destroyAsync(pid);
      }).then(function(args) {
        console.log('----', args);
        assert.equal(args.n, 1);
        assert.equal(args.ok, 1);
      }).catch(function(error) {
        console.log('Error: ', error);
      });
    });
  });

});
