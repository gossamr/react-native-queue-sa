import _ from 'lodash';

/**
 * DB Implementation that stores the jobs by id in memory.
 *
 * Appropriate for producer/consumer pattern with one of each.
 */

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

storage primitives:
  get
  save
  delete
  getKeys

*/

const JOB_PREFIX = '@queue:Job-';

export const memoryStorage = {
  items: {},
  /**
	 * Get a one or more value for a key or array of keys from AsyncStorage
	 * @param {String|Array} key A key or array of keys
	 * @return {Promise}
	 */
  get: async (key) => {
    if(!Array.isArray(key)) {
      if (memoryStorage.items[key]) {
        return memoryStorage.items[key]
      } else {
        return null
      }
    }
    else {
      let result = [];
      for (const k of key) {
        const value = memoryStorage.items[k];
        if (value) result.push(value);
      }
      return result;
    }
  },

  /**
	 * Save a key value pair or a series of key value pairs to AsyncStorage.
	 * @param  {String|Array} key The key or an array of key/value pairs
	 * @param  {Any} value The value to save
	 * @return {Promise}
	 */
  save: async (key, value) => {
    if(!Array.isArray(key)) {
      return memoryStorage.items[key] = value
    } else {
      await key.map(async (pair) => {
        memoryStorage.items[pair[0]] = pair[1];
      });
      return [];
    }
  },

  /**
	 * Delete the value for a given key in AsyncStorage.
	 * @param  {String|Array} key The key or an array of keys to be deleted
	 * @return {Promise}
	 */
  delete: async (key) => {
    if (Array.isArray(key)) {
      let res = [];
      for (const k of key) {
        const r = delete memoryStorage.items[k]
        res.push(r);
      }
      return res;
    } else {
      return delete memoryStorage.items[key]
    }
  },

  /**
	 * Push a value onto an array stored in AsyncStorage by key or create a new array in AsyncStorage for a key if it's not yet defined.
	 * @param {String} key They key
	 * @param {Any} value The value to push onto the array
	 * @return {Promise}
	 */
  push: async (key, value) => {
    return memoryStorage.get(key).then((currentValue) => {
      if (currentValue === null) {
        // if there is no current value populate it with the new value
        return memoryStorage.save(key, [value]);
      }
      if (Array.isArray(currentValue)) {
        currentValue.push(value)
        return memoryStorage.save(key, currentValue);
      }
      throw new Error(`Existing value for key "${key}" must be of type null or Array, received ${typeof currentValue}.`);
    });
  },

  /**
   * Gets all keys known to the app, for all callers, libraries, etc
   * @return the array of keys in the storage
   */
  getKeys: async (prefix = null) => {
    let result = Object.keys(memoryStorage.items);
    if(prefix){
      result = result.filter((e)=>e.startsWith(prefix));
    }
    return result;
  },

  /**
   * Deletes all the items in the memoryStorage.
   */
  deleteAll: async ()=>{
    memoryStorage.items = {};
  }
};

export default class MemoryStorage {

  async init(){
    // Nothing to be done
  }

  _getKey(job) {
    return JOB_PREFIX + job.id;
  }

  async create(job) {
    return memoryStorage.save(this._getKey(job), job);
  }

  async objects() {
    let keys = await memoryStorage.getKeys(JOB_PREFIX);
    return memoryStorage.get(keys);
  }

  async save(job) {
    return this.create(job);
  }

  async saveAll(jobs) {
    let pairs = jobs.map(j => [this._getKey(j), j]);
    return memoryStorage.save(pairs);
  }

  async delete(job) {
    if(Array.isArray(job)){
      return memoryStorage.delete(job.map(j => this._getKey(j)));
    }else{
      return memoryStorage.delete(this._getKey(job));
    }
  }

  async deleteAll() {
    let keys = await memoryStorage.getKeys(JOB_PREFIX);
    return memoryStorage.delete(keys);
  }

  async merge(job, fields){
    Object.assign(job, fields);
    return this.save(job);
  }

  /* Queries */
  async findNextJobs(timeoutUpperBound){
    let jobs = await this.objects();
    jobs = (timeoutUpperBound !== undefined)
      ? jobs.filter(j => (!j.active && j.failed === null && j.timeout > 0 && j.timeout < timeoutUpperBound))
      : jobs.filter(j => (!j.active && j.failed === null));
    jobs = _.orderBy(jobs, ['priority', 'created'], ['desc', 'asc']);
    return jobs;
  }

  async markActive(jobs){
    // Mark concurrent jobs as active
    jobs.forEach( job => {
      job.active = true;
    });

    return this.saveAll(jobs);
  }

  async deleteByName(jobName){
    let jobs = await this.objects();
    jobs = jobs.filter(j => j.name === jobName);

    if (jobs.length) {
      return this.delete(jobs);
    }
  }

  async resetFailedJobs(){
    const jobs = await this.objects();

    jobs.forEach(job => job.failed = null);

    return this.saveAll(jobs);
  }
}
