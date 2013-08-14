/*!
 * Connect - Redis
 * Copyright(c) 2012 TJ Holowaychuk <tj@vision-media.ca>
 * MIT Licensed
 */

/**
 * Module dependencies.
 */

var redis = require('redis')
  , debug = require('debug')('connect:redis');

/**
 * One day in seconds.
 */

var oneDay = 86400;

/**
 * Return the `RedisStore` extending `connect`'s session Store.
 *
 * @param {object} connect
 * @return {Function}
 * @api public
 */

module.exports = function(connect){

  /**
   * Connect's Store.
   */

  var Store = connect.session.Store;

  /**
   * Initialize RedisStore with the given `options`.
   *
   * @param {Object} options
   * @api public
   */

  function RedisStore(options) {
    var self = this;

    options = options || {};
    Store.call(this, options);
    this.prefix = null == options.prefix
      ? 'sess:'
      : options.prefix;

    this.client = options.client || new redis.createClient(options.port || options.socket, options.host, options);
    if (options.pass) {
      this.client.auth(options.pass, function(err){
        if (err) throw err;
      });    
    }

    this.ttl =  options.ttl;
    this.setexCompatible = options.setexCompatible;

    if (options.db) {
      self.client.select(options.db);
      self.client.on("connect", function() {
        self.client.send_anyways = true;
        self.client.select(options.db);
        self.client.send_anyways = false;
      });
    }

    self.client.on('error', function () { self.emit('disconnect'); });
    self.client.on('connect', function () { self.emit('connect'); });
  };

  /**
   * Inherit from `Store`.
   */

  RedisStore.prototype.__proto__ = Store.prototype;

  /**
   * Attempt to fetch session by the given `sid`.
   *
   * @param {String} sid
   * @param {Function} fn
   * @api public
   */

  RedisStore.prototype.get = function(sid, fn){
    var that = this;
    sid = this.prefix + sid;
    debug('HGETALL "%s"', sid);
    that.client.hgetall(sid, function(err, data){
      if (err || !data && that.setexCompatible) {
          debug('GET "%s"', sid);
          return that.client.get(sid, function (err, data) {
              if (err) return fn(err);
              if (!data) return fn();
              var result;
              data = data.toString();
              debug('GOT %s', data);
              try {
                result = JSON.parse(data);
              } catch (err) {
                return fn(err);
              }
              result._original = '';
              return fn(null, result);
          });
      }
      if (err) return fn(err);
      if (!data) return fn();
      debug('GOT %s', JSON.stringify(data));
      Object.keys(data).forEach(function (key) {
          data[key] = JSON.parse(data[key]);
      });
      data._original = JSON.stringify(data);
      return fn(null, data);
    });
  }; 
  /**
   * Commit the given `sess` object associated with the given `sid`.
   *
   * @param {String} sid
   * @param {Session} sess
   * @param {Function} fn
   * @api public
   */

  RedisStore.prototype.set = function(sid, sess, fn){
    sid = this.prefix + sid;
    try {
      var maxAge = sess.cookie.maxAge
        , ttl = this.ttl
        , original = (sess._original ? JSON.parse(sess._original) : {})
        , origKeys = Object.keys(original)
        , newKeys = Object.keys(sess)
        , origIndex
        , hmset = {}
        , hdel = [sid];
      origIndex = newKeys.indexOf('_original');
      if (origIndex >= 0) {
          newKeys.splice(origIndex, 1);
      }

      ttl = ttl || ('number' == typeof maxAge
          ? maxAge / 1000 | 0
          : oneDay);

      if (origKeys.length) {
          origKeys.forEach(function (key) {
              if (newKeys.indexOf(key) < 0) {
                  hdel.push(key);
              }
          });
          newKeys.forEach(function (key) {
              var val = JSON.stringify(sess[key]);
              if (!(original[key] && JSON.stringify(original[key]) === val)) {
                  hmset[key] = val;
              }
          });
          if (hdel.length > 1) {
              this.client.send_command('hdel', hdel, function (err) {
                  err || debug('HDEL complete');
                  if (err) {
                      console.log(err);
                  }
              });
          }
          debug('HMSET "%s" ttl:%s %s', sid, ttl, sess);
          this.client.hmset(sid, hmset, function(err){
            err || debug('HMSET complete');
            fn && fn.apply(this, arguments);
          });
          this.client.expire(sid, ttl, function (err) {
              err || debug('EXPIRE complete');
          });
      } else {
          hmset = {};
          Object.keys(sess).forEach(function (key) {
              hmset[key] = JSON.stringify(sess[key]);
          });
          this.client.hmset(sid, hmset, function(err){
            err || debug('HMSET complete');
            fn && fn.apply(this, arguments);
          });
          this.client.expire(sid, ttl, function (err) {
              err || debug('EXPIRE complete');
          });
      }
    } catch (err) {
      fn && fn(err);
    } 
  };

  /**
   * Destroy the session associated with the given `sid`.
   *
   * @param {String} sid
   * @api public
   */

  RedisStore.prototype.destroy = function(sid, fn){
    sid = this.prefix + sid;
    this.client.del(sid, fn);
  };

  return RedisStore;
};
