'use strict';

const Riak = require('basho-riak-client');
const _ = require('lodash');

class Client {
  /**
   * Creates a new client with config,
   * and adds delegated methods
   * @param config
   */
  constructor(config) {
    this.config = _.defaults({}, config.cluster, {
      // need to create new copy of config
      // riak decides to replace nodes with node objects
      nodes: Riak.Node.buildNodes(config.nodes, config.nodeOptions)
    });

    /**
     * Whether or not this cluster is connected
     * @type {boolean}
     * @private
     */
    this._isConnected = false;

    /**
     * The Riak client
     * @type {Riak.Client}
     */
    this.client = new Riak.Client(new Riak.Cluster(this.config), (err) => {
      if (err) {
        throw new Error(err);
      }

      this._isConnected = true;
    });

    // Delegate methods
    delegate(this, 'deleteIndex');
    delegate(this, 'deleteValue');
    delegate(this, 'execute');
    delegate(this, 'fetchBucketProps');
    delegate(this, 'fetchBucketTypeProps');
    delegate(this, 'fetchCounter');
    delegate(this, 'fetchIndex');
    delegate(this, 'fetchMap');
    delegate(this, 'fetchPreflist');
    delegate(this, 'fetchSchema');
    delegate(this, 'fetchSet');
    delegate(this, 'fetchValue');
    delegate(this, 'ListBuckets');
    delegate(this, 'listKeys');
    delegate(this, 'mapReduce');
    delegate(this, 'ping');
    delegate(this, 'resetBucketProps');
    delegate(this, 'search');
    delegate(this, 'secondaryIndexQuery');
    delegate(this, 'storeBucketProps');
    delegate(this, 'storeBucketTypeProps');
    delegate(this, 'storeIndex');
    delegate(this, 'storeSchema');
    delegate(this, 'storeValue');
    delegate(this, 'tsDelete');
    delegate(this, 'tsDescribe');
    delegate(this, 'tsGet');
    delegate(this, 'tsListKeys');
    delegate(this, 'tsQuery');
    delegate(this, 'tsStore');
    delegate(this, 'updateCounter');
    delegate(this, 'updateMap');
    delegate(this, 'updateSet');
  }

  /**
   * Stop the client
   */
  stop() {
    this._isConnected = false;
    this.client.stop();
  }
}

module.exports = Client;


/**
 * Delegate the function to the client, but
 * add a connection check first
 *
 * @param {Client} client - the sand-riak client
 * @param {string} fn - function name
 */
function delegate(client, fn) {
  // Add this function to the current client
  // and wrap with a connection check
  client[fn] = function(...args) {
    let self = this;
    let p = null;
    if (sand.profiler && sand.profiler.enabled) {
      // Build the profiler request
      let req = `riak ${fn} `;
      if (args[0].bucketType) {
        req += `types/${args[0].bucketType} `;
      }

      if (args[0].bucket) {
        req += `bucket/${args[0].bucket} `;
      }

      // if (args[0].key) {
      //   req += `bucket/${args[0].key} `;
      // }

      if (args[0].indexName) {
        req += `search/${args[0].indexName} `;
      }

      if (args[0].q) {
        req += `query/${args[0].q.replace(/(\w+):\w+/ig, '$1:*')}`
      }

      p = sand.profiler.profile(req.trim());
    }

    return new Promise(function(resolve, reject) {

      function returnResult(err, response, data) {
        p && p.stop();
        if (err) {
          err = new Error(err);
          err.data = data;
          err.req = `${fn}: ${args[0].join(', ')}`;
          sand.riak.error(`${err.message} ${fn}:`, ...args);
          return reject(err);
        }

        resolve(response);
      }

      if (!self._isConnected) {
        // We are connected so lets execute function
        client.client[fn](...args, returnResult);
        return;
      }
      // we need to wait for connection

      // set timeout for taking too long to connect
      let timer = setTimeout(checkIsConnected, self.config.connectTimeout);

      /**
       * Listen to state change and run function
       * on connection
       */
      let checkConnectState = function() {
        // We are running
        if (client.client.cluster.state == Riak.Cluster.State.RUNNING) {
          // Set to connected
          self._isConnected = true;

          // clear timeout
          clearTimeout(timer);

          // Remove listener
          client.client.cluster.removeListener('stateChange', checkConnectState);

          // call the function
          client.client[fn](...args, returnResult);
        }
      };

      // Listen to state changes
      self.client.cluster.on('stateChange', checkConnectState);
      checkConnectState();
    }).catch(sand.error);
  };
}

/**
 * We check if the client is connected
 * and throw error if not
 */
function checkIsConnected() {
  if (!this._isConnected) {
    throw Error('Could not connect to riak server');
  }
}