/**
 * Module Dependencies
 */

"use strict";

const SandGrain = require('sand-grain');
const Q = require('q');
const _ = require('lodash');
const Client = require('./Client');
const Riak = require('basho-riak-client');

/**
 * Expose `Riak`
 */
class RiakGrain extends SandGrain {
  constructor() {
    super();
    this.name = this.configName = 'riak';
    this.defaultConfig = require('./default');
    this.version = require('../package').version;
  }

  bindToContext(ctx) {
    ctx.riak = new Client(this.config);

    ctx.on('end', function() {
      try {
        ctx.riak.stop();
      } catch (e) {}
    });
  }

  get(bucket, key, cb) {
    return wrapCallInPromise.call(this, 'fetchValue', { bucket, key }, cb);
  }

  save(bucket, key, value, cb) {
    return wrapCallInPromise.call(this, 'storeValue', { bucket, key, value }, cb);
  }

  delete(bucket, key, cb) {
    return wrapCallInPromise.call(this, 'deleteValue', { bucket, key }, cb);
  }
}

function wrapCallInPromise(fn, args, cb) {
  let client = new Client(this.config);

  if (this.config.bucketType) {
    args.bucketType = this.config.bucketType;
  }
  
  function stop() {
    client.stop();
  }

  if ('function' === typeof cb) {
    cb = process.domain ? process.domain.bind(cb) : cb;
    return client[fn].call(client, args)
      .then((response) => {
        cb(null, response);
        stop();
      }).catch((error) => {
        cb(error);
        stop();
      });
  }

  return Q.nfcall(client[fn].bind(client), args).then((res) => {
    stop();
    return res;
  }).catch((e) => {
    stop();
    return e;
  });
}

module.exports = RiakGrain;
RiakGrain.Riak = Riak;