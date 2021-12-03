/**
 * Realm database bootstrap
 */

import { Config } from '../config/config';
import Realm from 'realm';

const JobSchema = {
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
};

export default class RealmStorage {

  static realmInstance = null; // Use a singleton connection to realm for performance.

  static async getRealmInstance(options = {}) {
    // Connect to realm if database singleton instance has not already been created.
    if (RealmStorage.realmInstance === null) {

      RealmStorage.realmInstance = await Realm.open({
        path: options.realmPath || Config.REALM_PATH,
        schemaVersion: Config.REALM_SCHEMA_VERSION,
        schema: [JobSchema]

        // Look up shouldCompactOnLaunch to auto-vacuum https://github.com/realm/realm-js/pull/1209/files
      });
    }

    return RealmStorage.realmInstance;
  }

  async init(){
    if (!this.realm) {
      this.realm = await RealmStorage.getRealmInstance();
    }
  }

  async create(job) {
    this.realm.write(() => {
      this.realm.create('Job', job);
    });
  }

  async objects(sync = true) {
    if (!sync)
        return this.realm.objects('Job');
    let jobs = null;
    this.realm.write(() => {
        jobs = this.realm.objects('Job');
    });

    return jobs;
  }

  async save(job) {
    this.realm.write(() => {
      this.realm.create('Job', job, true);
    });
  }

  async saveAll(jobs) {
    this.save(jobs);
  }

  async delete(job) {
    this.realm.write(() => {
      this.realm.delete(job);
    });
  }

  async deleteAll() {
    this.realm.write(() => {
      this.realm.deleteAll();
    });
  }

  async merge(job, fields){
    this.realm.write(()=>{
      Object.assign(job, fields);
    });
  }

  /* Queries */

  /**
   * Finds the next jobs that are active and not failed
   *
   * @param {*} timeoutUpperBound the maximum timeout value to select. If timeout <= 0, no record will be returned.
   * If undefined, the criteria is ignored.
   */
  async findNextJobs(timeoutUpperBound){
    const initialQuery = (timeoutUpperBound !== undefined)
    ? 'active == FALSE AND failed == null AND timeout > 0 AND timeout < ' + timeoutUpperBound
    : 'active == FALSE AND failed == null';

    return this.realm.objects('Job')
        .filtered(initialQuery)
        .sorted([['priority', true], ['created', false]]);
  }

  async markActive(jobs){
    this.realm.write(() => {
        jobs.forEach( job => job.active = true);
    });
  }

  async deleteByName(jobName){
    this.realm.write(() => {
        let jobs = this.realm.objects('Job')
        .filtered('name == "' + jobName + '"');

        if (jobs.length) {
            this.realm.delete(jobs);
        }
    });
  }

  async resetFailedJobs(){
    const jobs = await this.objects();

    this.realm.write(() => {
        jobs.forEach(job => job.failed = null);
    });
  }

}
