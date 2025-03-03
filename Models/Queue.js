/**
 *
 * Queue Model
 *
 */

import uuid from 'react-native-uuid';
import promiseReflect from 'promise-reflect';

import Worker from './Worker';

export class Queue {

  /**
   *
   * Set initial class properties.
   *
   * @constructor
   * @param storageFactory {()=>Storage} - Factory method returning the storage to be used (defaults to RealmStorage)
   * @param executeFailedJobsOnStart {boolean} - Indicates if previously failed jobs will be executed on start (actually when created new job).
   */
  constructor(storageFactory, executeFailedJobsOnStart = false) {
    this.storageFactory = storageFactory;
    this.jobDB = null;
    this.worker = new Worker();
    this.status = 'inactive';
    this.executeFailedJobsOnStart = executeFailedJobsOnStart;
  }

  /**
   *
   * Initializes the queue by connecting to jobDB database.
   *
   */
  init = async () => {
    if (this.jobDB === null) {
      this.jobDB = this.storageFactory();
      await this.jobDB.init();
    }
  }

  /**
   *
   * Add a worker function to the queue.
   *
   * Worker will be called to execute jobs associated with jobName.
   *
   * Worker function will receive job id and job payload as parameters.
   *
   * Example:
   *
   * function exampleJobWorker(id, payload) {
   *  console.log(id); // UUID of job.
   *  console.log(payload); // Payload of data related to job.
   * }
   *
   * @param jobName {string} - Name associated with jobs assigned to this worker.
   * @param worker {function} - The worker function that will execute jobs.
   * @param options {object} - Worker options. See README.md for worker options info.
   */
  addWorker(jobName, worker, options = {}) {
    this.worker.addWorker(jobName, worker, options);
  }

  /**
   *
   * Delete worker function from queue.
   *
   * @param jobName {string} - Name associated with jobs assigned to this worker.
   */
  removeWorker(jobName) {
    this.worker.removeWorker(jobName);
  }

  /**
   *
   * Creates a new job and adds it to queue.
   *
   * Queue will automatically start processing unless startQueue param is set to false.
   *
   * @param name {string} - Name associated with job. The worker function assigned to this name will be used to execute this job.
   * @param payload {object} - Object of arbitrary data to be passed into worker function when job executes.
   * @param options {object} - Job related options like timeout etc. See README.md for job options info.
   * @param startQueue - {boolean} - Whether or not to immediately begin prcessing queue. If false queue.start() must be manually called.
   */
  async createJob(name, payload = {}, options = {}, startQueue = true) {

    if (!name) {
      throw new Error('Job name must be supplied.');
    }

    // Validate options
    if (options.timeout < 0 || options.attempts < 0) {
      throw new Error('Invalid job option.');
    }

    // here we reset `failed` prop
    if (this.executeFailedJobsOnStart) {
      this.jobDB.resetFailedJobs();

      this.executeFailedJobsOnStart = false;
    }

    await this.jobDB.create({
      id: payload.id || uuid.v4(),
      name,
      payload: JSON.stringify(payload),
      data: JSON.stringify({
        attempts: options.attempts || 1
      }),
      priority: options.priority || 0,
      active: false,
      timeout: (options.timeout >= 0) ? options.timeout : 25000,
      created: new Date(),
      failed: null,
    });

    // Start queue on job creation if it isn't running by default.
    if (startQueue && this.status === 'inactive') {
      this.start();
    }

  }

  /**
   *
   * Start processing the queue.
   *
   * If queue was not started automatically during queue.createJob(), this
   * method should be used to manually start the queue.
   *
   * If queue.start() is called again when queue is already running,
   * queue.start() will return early with a false boolean value instead
   * of running multiple queue processing loops concurrently.
   *
   * Lifespan can be passed to start() in order to run the queue for a specific amount of time before stopping.
   * This is useful, as an example, for OS background tasks which typically are time limited.
   *
   * NOTE: If lifespan is set, only jobs with a timeout property at least 500ms less than remaining lifespan will be processed
   * during queue processing lifespan. This is to buffer for the small amount of time required to query for suitable
   * jobs, and to mark such jobs as complete or failed when job finishes processing.
   *
   * IMPORTANT: Jobs with timeout set to 0 that run indefinitely will not be processed if the queue is running with a lifespan.
   *
   * @param lifespan {number} - If lifespan is passed, the queue will start up and run for lifespan ms, then queue will be stopped.
   * @return {boolean|undefined} - False if queue is already started. Otherwise nothing is returned when queue finishes processing.
   */
  async start(lifespan) {

    // If queue is already running, don't fire up concurrent loop.
    if (this.status == 'active') {
      return false;
    }

    this.status = 'active';

    // Get jobs to process
    const startTime = Date.now();
    let lifespanRemaining = null;
    let concurrentJobs = [];



    do{
      if (lifespan !== undefined) {
        lifespanRemaining = lifespan - (Date.now() - startTime);
        concurrentJobs = await this.getConcurrentJobs(lifespanRemaining);
      } else {
        concurrentJobs = await this.getConcurrentJobs();
      }

      // Loop over jobs and process them concurrently.
      const processingJobs = concurrentJobs.map( job => {
        return this.processJob(job);
      });

      // Promise Reflect ensures all processingJobs resolve so
      // we don't break await early if one of the jobs fails.
      await Promise.all(processingJobs.map(promiseReflect));

    } while (this.status === 'active' && concurrentJobs.length);

    this.status = 'inactive';

  }

  /**
   *
   * Stop processing queue.
   *
   * If queue.stop() is called, queue will stop processing until
   * queue is restarted by either queue.createJob() or queue.start().
   *
   */
  stop() {
    this.status = 'inactive';
  }

  /**
   *
   * Get a collection of all the jobs in the queue.
   *
   * @param sync {boolean} - This should be true if you want to guarantee job data is fresh. Otherwise you could receive job data that is not up to date if a write transaction is occuring concurrently.
   * @return {promise} - Promise that resolves to a collection of all the jobs in the queue.
   */
  async getJobs(sync = true) {
    return this.jobDB.objects(sync);
  }

  /**
   *
   * Get the next job(s) that should be processed by the queue.
   *
   * If the next job to be processed by the queue is associated with a
   * worker function that has concurrency X > 1, then X related (jobs with same name)
   * jobs will be returned.
   *
   * If queue is running with a lifespan, only jobs with timeouts at least 500ms < than REMAINING lifespan
   * AND a set timeout (ie timeout > 0) will be returned. See Queue.start() for more info.
   *
   * @param queueLifespanRemaining {number} - The remaining lifespan of the current queue process (defaults to indefinite).
   * @return {promise} - Promise resolves to an array of job(s) to be processed next by the queue.
   */
  async getConcurrentJobs(queueLifespanRemaining) {
    // Get next job from queue.
    let nextJob = null;


    let timeoutUpperBound = undefined;
    let jobs;
    if(queueLifespanRemaining !== undefined){
      timeoutUpperBound = queueLifespanRemaining - 499; // Only get jobs with timeout at least 500ms < queueLifespanRemaining.
    }
    jobs = await this.jobDB.findNextJobs(timeoutUpperBound);

    if (jobs.length) {
      nextJob = jobs[0];
    }

    // If next job exists, get concurrent related jobs appropriately.
    if (!nextJob)
      return [];
    const concurrency = this.worker.getConcurrency(nextJob.name);

    let allRelatedJobs = jobs.filter(j => j.name === nextJob.name);

    let concurrentJobs = allRelatedJobs.slice(0, concurrency);

    this.jobDB.markActive(concurrentJobs);

    return concurrentJobs;
  }

  /**
   *
   * Process a job.
   *
   * Job lifecycle callbacks are called as appropriate throughout the job processing lifecycle.
   *
   * Job is deleted upon successful completion.
   *
   * If job fails execution via timeout or other exception, error will be
   * logged to job.data.errors array and job will be reset to inactive status.
   * Job will be re-attempted up to the specified "attempts" setting (defaults to 1),
   * after which it will be marked as failed and not re-attempted further.
   *
   * @param job {object} - Job model object
   */
  async processJob(job) {

    // Data must be cloned off the job object for several lifecycle callbacks to work correctly.
    // This is because job is deleted before some callbacks are called if job processed successfully.
    // More info: https://github.com/billmalarky/react-native-queue/issues/2#issuecomment-361418965
    const jobName = job.name;
    const jobId = job.id;
    const jobPayload = JSON.parse(job.payload);

    // Fire onStart job lifecycle callback
    this.worker.executeJobLifecycleCallback('onStart', jobName, jobId, jobPayload);
    let jobResult;
    try {

      jobResult = await this.worker.executeJob(job);

      // On successful job completion, remove job
      await this.jobDB.delete(job);

      // Job has processed successfully, fire onSuccess and onComplete job lifecycle callbacks.
      this.worker.executeJobLifecycleCallback('onSuccess', jobName, jobId, jobPayload, jobResult);
      this.worker.executeJobLifecycleCallback('onComplete', jobName, jobId, jobPayload, jobResult);

    } catch (error) {

      // Handle job failure logic, including retries.
      let jobData = JSON.parse(job.data);

      // Increment failed attempts number
      if (!jobData.failedAttempts) {
        jobData.failedAttempts = 1;
      } else {
        jobData.failedAttempts++;
      }

      // Log error
      if (!jobData.errors) {
        jobData.errors = [ error.message ];
      } else {
        jobData.errors.push(error.message);
      }

      let jobMerge = {};
      jobMerge.data = JSON.stringify(jobData);

      // Reset active status
      jobMerge.active = false;

      // Mark job as failed if too many attempts
      if (jobData.failedAttempts >= jobData.attempts) {
        jobMerge.failed = new Date();
      }

      await this.jobDB.merge(job, jobMerge);

      // Execute job onFailure lifecycle callback.
      this.worker.executeJobLifecycleCallback('onFailure', jobName, jobId, jobPayload);

      // If job has failed all attempts execute job onFailed and onComplete lifecycle callbacks.
      if (jobData.failedAttempts >= jobData.attempts) {
        this.worker.executeJobLifecycleCallback('onFailed', jobName, jobId, jobPayload);
        this.worker.executeJobLifecycleCallback('onComplete', jobName, jobId, jobPayload, jobResult);
      }

    }

  }

  /**
   *
   * Delete jobs in the queue.
   *
   * If jobName is supplied, only jobs associated with that name
   * will be deleted. Otherwise all jobs in queue will be deleted.
   *
   * @param jobName {string} - Name associated with job (and related job worker).
   */
  async flushQueue(jobName = null) {
    if (jobName) {
      return this.jobDB.deleteByName(jobName);
    } else {
      return this.jobDB.deleteAll();
    }
  }

}

/**
 *
 * Factory should be used to create a new queue instance.
 *
 * @param storageFactory {()=>Storage} - Factory method returning the storage to be used (defaults to RealmStorage)
 * @param executeFailedJobsOnStart {boolean} - Indicates if previously failed jobs will be executed on start (actually when created new job).
 *
 * @return {Queue} - A queue instance.
 */
export default async function queueFactory(storageFactory, executeFailedJobsOnStart = false) {

  const queue = new Queue(storageFactory, executeFailedJobsOnStart);
  await queue.init();

  return queue;

}
