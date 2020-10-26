/* eslint-disable no-unused-vars */
import {promisify} from 'util';
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {eratuontiFactory, COMMON_JOB_STATES, IMPORTER_JOB_STATES} from '@natlibfi/melinda-record-link-migration-commons';

export async function importToErätuonti(jobId, jobConfig, mongoOperator, amqpOperator, {apiUrl, apiUsername, apiPassword, apiClientUserAgent}) {
  const logger = createLogger();
  const setTimeoutPromise = promisify(setTimeout);
  const {linkDataHarvesterApiProfileId} = jobConfig;
  const eratuontiOperator = eratuontiFactory({apiUrl, apiUsername, apiPassword, apiClientUserAgent, linkDataHarvesterApiProfileId}); // eslint-disable-line no-unused-vars

  await mongoOperator.setState({jobId, state: IMPORTER_JOB_STATES.PROCESSING_ERATUONTI_IMPORT});

  await pumpMessagesFromQueue();
  // Await mongoOperator.setState({jobId, state: COMMON_JOB_STATES.DONE});

  return true;

  async function pumpMessagesFromQueue() {
    const amqpMessages = await amqpOperator.checkQueue(`${IMPORTER_JOB_STATES.PENDING_ERATUONTI_IMPORT}.${jobId}`);
    if (!amqpMessages) {
      amqpOperator.removeQueue(`${IMPORTER_JOB_STATES.PENDING_ERATUONTI_IMPORT}.${jobId}`);
      return;
    }

    // logger.log('silly', JSON.stringify(amqpMessages));

    const linkedData = amqpMessages.map(message => JSON.parse(message.content.toString()));
    logger.log('silly', JSON.stringify(linkedData));

    if (linkedData.length > 0) { // eslint-disable-line functional/no-conditional-statement
      const blobId = await eratuontiOperator.sendBlob(linkedData);

      // If there is error in connection to erätuonti wait 3secs and try again
      if (blobId === false) {
        await amqpOperator.nackMessages(amqpMessages);
        await setTimeoutPromise(3000);
        return pumpMessagesFromQueue();
      }

      // Link data has been sent to erätuonti
      logger.log('debug', blobId);
      await mongoOperator.pushBlobIds({jobId, blobIds: [blobId]});
      await mongoOperator.setState({jobId, state: COMMON_JOB_STATES.PENDING_ERATUONTI});
      await amqpOperator.ackMessages(amqpMessages);

      return pumpMessagesFromQueue();
    }

    // Await mongoOperator.setState({jobId, state: JOB_STATES.DONE});
    await amqpOperator.ackMessages(amqpMessages);
    return pumpMessagesFromQueue();
  }
}
