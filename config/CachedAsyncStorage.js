import _ from 'lodash';
import storage from './Storage';

/**
 * DB imitation based on array with help of RN AsyncStorage
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

*/

const BACKUP_TIME = 50;
const Job = '@queue:Job';

export default class CachedAsyncStorage {

  async init(){
    // await storage.delete(Job); // to delete all jobs
    await this._restore();
    await this._backup();
  }

  async _restore(){
    const jobDB = await storage.get(Job);
    this.db = jobDB || [];
  }

  async _backup(){
    await storage.save(Job, this.db.slice());

    setTimeout(await this._backup, BACKUP_TIME);
  }

  create(obj){
    let shouldSkip = false; // if obj.id is already in array

    for (let i = 0; i < this.db.length; i += 1) {
      if (this.db[i] === obj.id) shouldSkip = true;
    }

    if (!shouldSkip) this.db.push(obj);
  }

  objects(){
    return this.db.slice();
  }

  save(obj){
    for (let i = 0; i < this.db.length; i += 1) {
      if (this.db[i] === obj.id) this.db[i] = obj;
    }
  }

  saveAll(objs){
    objs.forEach(o => this.save(o));
  }

  delete(obj){
    if(!Array.isArray(obj)) {
      this.db = this.db.filter((el)=> el.id !== obj.id);
    }else{
      let ids = obj.map(a => a.id);
      this.db = this.db.filter((el)=> ids.indexOf(el.id)===-1);
    }
  }

  deleteAll(){
    this.db = [];
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