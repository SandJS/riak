/**
 * Module Dependencies
 */

"use strict";

const SandGrain = require('sand-grain');
const Client = require('@sazze/riak');
const Q = require('q');

/**
 * Expose `Riak`
 */
class Riak extends SandGrain {
  constructor() {
    super();
    this.name = this.configName = 'riak';
    this.defaultConfig = require('./default');
    this.version = require('../package').version;
    this.clients = {};
  }

  getClient(bucket) {
    if (!this.clients[bucket]) {
      this.clients[bucket] = this.newClient(bucket);
    }

    return this.clients[bucket]
  }

  newClient(bucket) {
    let config = _.merge({
      bucket: bucket
    }, this.config);

    return new Client(config);
  }

  get(bucket, key, cb) {
    return wrapCallInPromise.call(this, 'get', arguments);
  }

  save(bucket, key, value, headers, cb) {
    return wrapCallInPromise.call(this, 'put', arguments);
  }

  delete(bucket, key, cb) {
    return wrapCallInPromise.call(this, 'del', arguments);
  }
}

function wrapCallInPromise(fn, args) {
  let bucket = args[0];
  args = Array.prototype.splice.call(args, 1);
  let client = this.getClient(bucket);

  if ('function' === typeof args[args.length-1]) {
    return client[fn].apply(client, args);
  }

  return Q.nfapply(client[fn].bind(client), args);
}

exports = module.exports = Riak;