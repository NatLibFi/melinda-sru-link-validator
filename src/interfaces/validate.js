/* eslint-disable no-unused-vars */
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {MarcRecord} from '@natlibfi/marc-record';
import {createValidationFactory, recordActions, COMMON_JOB_STATES, VALIDATOR_JOB_STATES, IMPORTER_JOB_STATES} from '@natlibfi/melinda-record-link-migration-commons';

export async function validations(jobId, jobConfig, mongoOperator, amqpOperator) {
  const logger = createLogger();
  const {filterRecordsBy} = recordActions();

  const {sourceRecord, linkDataHarvesterValidationFilters} = jobConfig;
  const validators = await pumpValidators(linkDataHarvesterValidationFilters);
  const marcSourceRecord = new MarcRecord(sourceRecord);

  await pumpMessagesFromQueue(marcSourceRecord, validators);

  // Check recults
  const messages = await amqpOperator.checkQueue(`${IMPORTER_JOB_STATES.PENDING_ERATUONTI_IMPORT}.${jobId}`, 'messages');
  if (messages.length === 0 || !messages) {
    logger.log('debug', 'All records validated. No records for erätuonti! -----');
    await mongoOperator.setState({jobId, state: COMMON_JOB_STATES.DONE});
    await amqpOperator.removeQueue(`${VALIDATOR_JOB_STATES.PENDING_VALIDATION_FILTERING}.${jobId}`);
    return;
  }

  logger.log('debug', `All records validated. ${messages} to erätuonti! *****`);
  await mongoOperator.setState({jobId, state: IMPORTER_JOB_STATES.PENDING_ERATUONTI_IMPORT});
  await amqpOperator.removeQueue(`${VALIDATOR_JOB_STATES.PENDING_VALIDATION_FILTERING}.${jobId}`);

  return true;

  // Init validators
  async function pumpValidators(linkDataHarvesterValidationFilters, filters = []) {
    const [linkDataHarvesterValidationFilter, ...rest] = linkDataHarvesterValidationFilters;
    if (linkDataHarvesterValidationFilter === undefined) {
      return filters;
    }

    const validatorFilter = await createValidationFactory(linkDataHarvesterValidationFilter);
    const ifFilter = linkDataHarvesterValidationFilter.if;
    const {changes} = linkDataHarvesterValidationFilter;

    return pumpValidators(rest, [...filters, {validatorFilter, ifFilter, changes}]);
  }

  // Loop all records from queue
  async function pumpMessagesFromQueue(marcSourceRecord, validators) {
    // Get records
    const amqpMessages = await amqpOperator.checkQueue(`${VALIDATOR_JOB_STATES.PENDING_VALIDATION_FILTERING}.${jobId}`);
    if (!amqpMessages) {
      return true;
    }

    const records = await amqpOperator.messagesToRecords(amqpMessages);
    logger.log('debug', `Got records from queue. ${records.length} records pumping in validation!`);

    // Validate and filter records
    const linkedValidData = await pumpValidation(validators, marcSourceRecord, records);
    const mergedLinkedValidData = await filterAndMerge(linkedValidData);

    // Return valid records to amqp
    logger.log('debug', `${mergedLinkedValidData.length} valid records merged to link data`);
    await pumpToAmqp(mergedLinkedValidData, jobId, `${IMPORTER_JOB_STATES.PENDING_ERATUONTI_IMPORT}.${jobId}`);
    await amqpOperator.ackMessages(amqpMessages);

    return pumpMessagesFromQueue(marcSourceRecord, validators);
  }

  async function pumpValidation(validators, marcSourceRecord, records, linkedValidData = []) {
    const [config, ...rest] = validators;
    if (config === undefined) {
      return linkedValidData;
    }

    logger.log('info', 'Validating and filtering!');
    // Filter records
    const filteredRecords = await filterRecordsBy(marcSourceRecord, records, config.ifFilter);

    if (filteredRecords.length === 0) {
      return pumpValidation(rest, marcSourceRecord, records, linkedValidData);
    }

    logger.log('info', `${filteredRecords.length} PASSED IF FILTER! ************************************`);
    // Validate records
    const valids = await validatePump(filteredRecords, config.validatorFilter);
    // Create linkData
    const newLinkedValidData = valids.map(record => ({sourceRecord: marcSourceRecord, changes: [...config.changes], record}));

    return pumpValidation(rest, marcSourceRecord, records, [...linkedValidData, ...newLinkedValidData]);

    async function validatePump(records, validate, valids = []) {
      const [record, ...rest] = records;
      if (record === undefined) {
        return valids;
      }

      const validateResults = await validate(record, {fix: false, validateFixes: false});
      logger.log('silly', `Record is valid: ${validateResults.valid}`);

      // Collect if valid
      if (validateResults.valid) {
        return validatePump(rest, validate, [...valids, record]);
      }

      return validatePump(rest, validate, valids);
    }
  }

  function filterAndMerge(linkedValidData, mergedLinkedValidData = []) {
    const [linkdata, ...rest] = linkedValidData;
    if (linkdata === undefined) {
      return mergedLinkedValidData;
    }

    const original = new MarcRecord(linkdata.record);
    const filteredChanges = rest.filter(data => {
      const compare = new MarcRecord(data.record);
      const match = original.equalsTo(compare);
      return match;
    }).map(data => data.changes).flat();

    // Logger.log('debug', `Filtered changes: ${JSON.stringify(filteredChanges)}`);

    if (filteredChanges.length > 0) {
      const merged = {sourceRecord: linkdata.sourceRecord, changes: [...filteredChanges, ...linkdata.changes], record: linkdata.record};

      const filteredRest = rest.filter(data => {
        const compare = new MarcRecord(data.record, {subfieldValues: false});
        const notMatch = !original.equalsTo(compare);
        return notMatch;
      });

      return filterAndMerge(filteredRest, [...mergedLinkedValidData, merged]);
    }

    return filterAndMerge(rest, [...mergedLinkedValidData, linkdata]);
  }

  async function pumpToAmqp(linkDatas, jobId, queue) {
    const [linkData, ...rest] = linkDatas;
    if (linkData === undefined) {
      return;
    }

    await amqpOperator.sendToQueue({queue, correlationId: jobId, data: linkData});

    return pumpToAmqp(rest, jobId);
  }
}
