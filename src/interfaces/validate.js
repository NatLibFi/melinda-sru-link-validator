/* eslint-disable no-unused-vars */
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {MarcRecord} from '@natlibfi/marc-record';
import {createValidationFactory, recordActions, COMMON_JOB_STATES, VALIDATOR_JOB_STATES, IMPORTER_JOB_STATES} from '@natlibfi/melinda-record-link-migration-commons';

export function validations(jobId, amqpOperator) {
  const logger = createLogger();
  const {filterRecordsBy} = recordActions();

  return {pumpValidators, pumpMessagesFromQueue, pumpValidation, pumpToAmqp, filterAndMerge};

  // Init validators
  async function pumpValidators(linkDataHarvesterValidationFilters, filters = []) {
    const [linkDataHarvesterValidationFilter, ...rest] = linkDataHarvesterValidationFilters;
    if (linkDataHarvesterValidationFilter === undefined) {
      return filters;
    }

    const validatorFilter = await createValidationFactory(linkDataHarvesterValidationFilter);
    // console.log(validatorFilter); // eslint-disable-line no-console
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
    // console.log(JSON.stringify(records)); // eslint-disable-line no-console
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

    const original = new MarcRecord(linkdata.record, {subfieldValues: false});
    const filteredChanges = rest.filter(data => {
      const compare = new MarcRecord(data.record, {subfieldValues: false});
      const isMatch = original.equalsTo(compare);
      // logger.log('debug', `Is match: ${isMatch}`);
      return isMatch;
    }).map(data => data.changes).flat();

    // Logger.log('debug', `Filtered changes: ${JSON.stringify(filteredChanges)}`);

    if (filteredChanges.length > 0) {
      logger.log('debug', 'Merging changes');
      const allChanges = [...filteredChanges, ...linkdata.changes].map(change => JSON.stringify(change));
      const uniqueChanges = [...new Set(allChanges)].map(change => JSON.parse(change));
      const merged = {sourceRecord: linkdata.sourceRecord, changes: uniqueChanges, record: linkdata.record};

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
