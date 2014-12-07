/**
 * Module Dependencies
 */

var SandGrain = require('sand-grain');
var Extend = require('sand-extend').Extend;
var riak = require('riak-js');

/**
 * Initialize a new `Riak`.
 *
 * @api public
 */
function Riak() {
  this.super();

  this.defaultConfig = require('./default');
  this.version = require('../package').version;
}

Extend(Riak, SandGrain, {
  name: 'riak',

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

/**
 * Expose `Riak`
 */
exports = module.exports = Riak;