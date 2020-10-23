/* eslint-disable no-unused-vars, no-undef, no-warning-comments, no-console */
import fixturesFactory, {READERS} from '@natlibfi/fixura';
import {MarcRecord} from '@natlibfi/marc-record';
import {expect} from 'chai';
import {promisify} from 'util';
import {validations} from './validate';

describe('Validate', () => {
  const {getFixture} = fixturesFactory(__dirname, '..', '..', 'test-fixtures', 'validate');
  const job = getFixture({components: ['job.json'], reader: READERS.JSON});
  const records = getFixture({components: ['records.json'], reader: READERS.JSON});
  const marcRecords = records.map(record => new MarcRecord(record));

  const {jobId, jobConfig} = job;
  const {sourceRecord, linkDataHarvesterValidationFilters} = jobConfig;
  const marcSourceRecord = new MarcRecord(sourceRecord);
  const amqpOperator = createAmqpOperator();
  const {pumpValidators, pumpValidation, filterAndMerge} = validations(jobId, amqpOperator);

  describe('PumpValidators', () => {
    it('Should transform string validators to functioning ones', async () => {
      const changes = getFixture({components: ['pumpValidators', 'changes.json'], reader: READERS.JSON});
      const ifFilter = getFixture({components: ['pumpValidators', 'ifFilter.json'], reader: READERS.JSON});
      const validators = await pumpValidators(linkDataHarvesterValidationFilters);
      validators.forEach(config => {
        expect(config.ifFilter).to.eql(ifFilter);
        expect(config.changes).to.eql(changes);
        expect(config).to.respondsTo('validatorFilter');
      });
    });
  });

  describe('PumpValidation', () => {
    it('Should filter 1st and 5th out of records and have 4th twice', async () => {
      const validators = await pumpValidators(linkDataHarvesterValidationFilters);
      const filtered = await pumpValidation(validators, marcSourceRecord, marcRecords);
      expect(filtered.length).to.eql(4);
    });
  });

  describe('FilterAndMerge', () => {
    it('Should merge 3th and 4th unique changes', async () => {
      const validators = await pumpValidators(linkDataHarvesterValidationFilters);
      const filtered = await pumpValidation(validators, marcSourceRecord, marcRecords);
      const merged = await filterAndMerge(filtered);
      expect(merged.length).to.eql(3);
      expect(merged[1].changes).to.eql(merged[2].changes);
    });
  });
});

function createAmqpOperator() {
  const setTimeoutPromise = promisify(setTimeout);
  return {
    ackMessages: () => setTimeoutPromise(5),
    sendToQueue: () => setTimeoutPromise(5)
  };
}
