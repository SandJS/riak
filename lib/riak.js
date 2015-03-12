/**
 * Module Dependencies
 */

"use strict";

var SandGrain = require('sand-grain');
var riak = require('riak-js');


/**
 * Expose `Riak`
 */
class Riak extends SandGrain {
  constructor() {
    super();
    this.name = this.configName = 'riak';
    this.defaultConfig = require('./default');
    this.version = require('../package').version;
  }

  getClient() {
    return riak(this.config);
  }

  get() {
    var client = this.getClient();
    client.get.apply(client, arguments);
  }

  save() {
    var client = this.getClient();
    client.save.apply(client, arguments);
  }

  delete() {
    var client = this.getClient();
    client.remove.apply(client, arguments);
  }
}

exports = module.exports = Riak;