/**
 * Module Dependencies
 */

var SandGrain = require('sand-grain');
var riak = require('riak-js');


/**
 * Expose `Riak`
 */
exports = module.exports = SandGrain.extend({

  name: 'riak',

  construct: function() {
    this.super();
    this.defaultConfig = require('./default');
    this.version = require('../package').version;
  },

  getClient: function() {
    return riak(this.config);
  },

  get: function() {
    var client = this.getClient();
    client.get.apply(client, arguments);
  },

  save: function() {
    var client = this.getClient();
    client.save.apply(client, arguments);
  }
});
