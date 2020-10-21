import {parseBoolean} from '@natlibfi/melinda-commons/';
import {readEnvironmentVariable} from '@natlibfi/melinda-backend-commons/';

// Api client variables
export const apiUrl = readEnvironmentVariable('API_URL');
export const apiUsername = readEnvironmentVariable('API_USERNAME');
export const apiPassword = readEnvironmentVariable('API_PASSWORD');
export const apiClientUserAgent = readEnvironmentVariable('API_CLIENT_USER_AGENT', {defaultValue: '_RECORD-LINK-MIGRATION'});

// SRU variables
export const SRU_URL = readEnvironmentVariable('SRU_URL', {defaultValue: ''});

// Mongo variables to job
export const mongoUrl = readEnvironmentVariable('MONGO_URI', {defaultValue: 'mongodb://127.0.0.1:27017/db'});

// AMQP variables
export const amqpUrl = readEnvironmentVariable('AMQP_URL', {defaultValue: 'amqp://127.0.0.1:5672/'});
export const AMQP_QUEUE_PURGE_ON_LOAD = readEnvironmentVariable('PURGE_QUEUE_ON_LOAD', {defaultValue: 1, format: v => parseBoolean(v)});
