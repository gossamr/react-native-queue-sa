
/**
 * DB Implementation that stores the jobs by id.
 * 
 * Appropriate for producer/consumer pattern with one of each.
 */

import storage from './Storage';

/*

=== SCHEMA ===

JobSchema = {
  name: 'Job',
  primaryKey: 'id',
  properties: {
    id:  'string', // UUID.
    name: 'string', // Job name to be matched with worker function.
    payload: 'string', // Job payload stored as JSON.
    data: 'string', // Store arbitrary data like "failed attempts" as JSON.
    priority: 'int', // -5 to 5 to indicate low to high priority.
    active: { type: 'bool', default: false}, // Whether or not job is currently being processed.
    timeout: 'int', // Job timeout in ms. 0 means no timeout.
    created: 'date', // Job creation timestamp.
    failed: 'date?' // Job failure timestamp (null until failure).
  }
}

=== ====== ===

*/

const JOB_PREFIX = '@queue:Job-';

export default class TxnAsyncStorage {
  init = async () => {
    // Nothing to be done
  };

  _getKey(job) {
    return JOB_PREFIX + job.id;
  }

  async create(job) {
    return storage.save(this._getKey(job), job);
  }

  async objects() {
    let keys = await storage.getKeys(JOB_PREFIX);
    return storage.get(keys);
  }

  async save(job) {
    return this.create(job);
  }

  async saveAll(jobs) {
    let pairs = jobs.map(j => [this._getKey(j), j]);
    return storage.save(pairs);
  }

  async delete(job) {
    if(Array.isArray(job)){
      return storage.delete(job.map(j => this._getKey(j)));
    }else{
      return storage.delete(this._getKey(job));
    }
  }

  async deleteAll() {
    let keys = await storage.getKeys(JOB_PREFIX);
    return storage.delete(keys);
  }
}
