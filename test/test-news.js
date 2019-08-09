const config = require('config');
const peter = require('../index').getManager();
const assert = require('assert');
const should = require('should');

describe('Test news', () => {

  before(function() {
    return peter.bindDbAsync(config.get('schema')).then(args => {
      return peter.createAsync('@User', {
        avatar: 'test-news',
        gender: 0,
        nick_name: 'test-news'
      });
    }).then(args => {
      return peter.createAsync('@News', {
        cover: 'test-news',
        title: 'test-news',
        posted_by: {
          user: args,
          nick: 'test-news',
          avatar: 'test-news'
        },
        n_like: 0,
        n_comment: 0
      });
    }).then(args => {
      should.exist(args);
    }).catch(error => {
      console.log('Error: ', error);
      should.not.exist(error);
    });
  });

  after(() => {
    return peter.findAsync('@News').then(args => {
      return Promise.all(args.map(item => {
        return peter.destroyAsync(item._id);
      }));
    }).then(args => {
      args.length.should.be.above(0);
    }).catch(error => {
      console.log('Error', error);
      assert.equal(error, null);
    });
  });

  describe('#news', () => {
    it('#add comment', () => {
      let user, news;
      return peter.findOneAsync('@User', {}).then(args => {
        should.exist(args);
        user = args;
        return peter.findOneAsync('@News', {});
      }).then(args => {
        return peter.createAsync('@NewsComment', {
          news: args._id,
          is_like: false,
          content: 'test-news',
          posted_by: {
            user: user._id,
            nick: user.nick_name,
            avatar: user.avatar
          }
        });
      }).then(args => {
        news = args;
        return peter.pushAsync(news, 'read_by', user._id);
      }).then(args => {
        return peter.pushAsync(news, 'read_by', user._id);
      }).catch(error => {
        console.log('Error: ', error);
        should.not.exist(error);
      })
    });

    it('#peter.add 单元测试', () => {
      return peter.findOneAsync('@News').then(args => {
        return peter.increaseAsync(args._id, 'n_like', 2);
      }).then(args => {
        args.n_like.should.be.above(1);
      }).catch(error => {
        console.log('TestError', error);
        assert(error === null);
      });
    });
    
  });

});