/* eslint-disable no-unused-vars, no-undef, no-warning-comments */
import {promisify} from 'util';
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {mongoFactory, COMMON_JOB_STATES, VALIDATOR_JOB_STATES, IMPORTER_JOB_STATES, amqpFactory} from '@natlibfi/melinda-record-link-migration-commons';
import {validations} from './interfaces/validate';
import {importToErätuonti} from './interfaces/eratuonti';
import {MarcRecord} from '@natlibfi/marc-record';

export default async function ({
  apiUrl, apiUsername, apiPassword, apiClientUserAgent, mongoUrl, amqpUrl
}) {
  const logger = createLogger();
  const eratuontiConfig = {apiUrl, apiUsername, apiPassword, apiClientUserAgent};
  const mongoOperator = await mongoFactory(mongoUrl);
  const amqpOperator = await amqpFactory(amqpUrl);
  const setTimeoutPromise = promisify(setTimeout);
  logger.log('info', 'Melinda-sru-harvester has started');

  return check();

  async function check(wait) {
    if (wait) {
      await setTimeoutPromise(3000);
      return check();
    }

    // If job state PROCESSING_SRU_HARVESTING => collect or , VALIDATING => validate or  => import continue it!

    // SEPARATE TO MICROSERVICE IMPORTER?
    // Import to erätuonti
    await checkJobsInState(IMPORTER_JOB_STATES.PROCESSING_ERATUONTI_IMPORT);
    await checkJobsInState(IMPORTER_JOB_STATES.PENDING_ERATUONTI_IMPORT);

    // SEPARATE TO MICROSERVICE VALIDATOR?
    // Validate and filter link data
    await checkJobsInState(VALIDATOR_JOB_STATES.PROCESSING_VALIDATION_FILTERING);
    await checkJobsInState(VALIDATOR_JOB_STATES.PENDING_VALIDATION_FILTERING);

    // Loop
    return check(true);
  }

  async function checkJobsInState(state) {
    const job = await mongoOperator.getOne(state);
    // logger.log('debug', JSON.stringify(job, undefined, 2));
    if (job === undefined || job === null) { // eslint-disable-line functional/no-conditional-statement
      logger.log('info', `No job in state: ${state}`);
      return;
    }

    /* Empty queue loop for testing
    const {jobId} = job;
    await mongoOperator.setState({jobId, state: COMMON_JOB_STATES.DONE});
    */

    // Real loop
    const {jobId, jobConfig} = job;

    // Import linked data to Erätuonti
    if (state === IMPORTER_JOB_STATES.PENDING_ERATUONTI_IMPORT || state === IMPORTER_JOB_STATES.PROCESSING_ERATUONTI_IMPORT) {
      await importToErätuonti(jobId, jobConfig, mongoOperator, amqpOperator, eratuontiConfig);
      return check();
    }

    // Validate potential link data
    if (state === VALIDATOR_JOB_STATES.PENDING_VALIDATION_FILTERING || state === VALIDATOR_JOB_STATES.PROCESSING_VALIDATION_FILTERING) {
      await runValidations(job, mongoOperator, amqpOperator);
      return check();
    }

    return check();
  }

  async function runValidations(job, mongoOperator, amqpOperator) {
    const {jobId, jobConfig} = job;
    const {sourceRecord, linkDataHarvesterValidationFilters} = jobConfig;
    const validationsOperator = validations(jobId, amqpOperator);

    const validators = await validationsOperator.pumpValidators(linkDataHarvesterValidationFilters);
    const marcSourceRecord = new MarcRecord(sourceRecord);

    await validationsOperator.pumpMessagesFromQueue(marcSourceRecord, validators);

    // Check recults
    const messages = await amqpOperator.checkQueue(`${IMPORTER_JOB_STATES.PENDING_ERATUONTI_IMPORT}.${jobId}`, 'messages');
    if (messages.length === 0 || !messages) {
      logger.log('debug', 'All records validated. No records for erätuonti! -----');
      await mongoOperator.setState({jobId, state: COMMON_JOB_STATES.DONE});
      return;
    }

    logger.log('debug', `All records validated. ${messages} to erätuonti! *****`);
    await mongoOperator.setState({jobId, state: IMPORTER_JOB_STATES.PENDING_ERATUONTI_IMPORT});
  }
}
