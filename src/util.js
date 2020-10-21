/* eslint-disable no-unused-vars */
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {format} from 'util';

const logger = createLogger(); // eslint-disable-line no-unused-vars

export function getFromRecord(from, record, collect = []) {
  const fields = record.get(new RegExp(`^${from.tag}$`, 'u'));

  if (fields.length === 0) {
    return false;
  }
  return fields.map(field => {
    // Logger.log('debug', JSON.stringify(field));
    // Get non subfield value
    if (from.value === 'value') {
      return field.value;
    }

    if (from.value === 'collect') {
      if (collect === []) {
        // Collect all
        return field.subfields;
      }
      // Collect marked ones
      return field.subfields.filter(sub => collect.includes(sub.code));
    }

    // Get subfield value
    const [subfield] = field.subfields.filter(sub => {
      if (sub.code === from.value.code) {
        return true;
      }
      return false;
    });

    return subfield.value;
  });
}

export function addToRecord(value, to, record) {
  logger.log('verbose', 'Adding value to record');
  const [field] = record.get(new RegExp(`^${to.tag}$`, 'u'));
  const formatedValue = format(to.format, value);
  logger.log('debug', formatedValue);

  if (field === undefined) {
    if (to.value === 'value') {
      record.insertField({
        tag: to.tag,
        value: formatedValue
      });

      return record;
    }

    record.insertField({
      tag: to.tag,
      subfields: [
        {
          code: to.value.code,
          value: formatedValue
        }
      ]
    });

    return record;
  }

  if (to.value === 'value') {
    field.value = formatedValue; // eslint-disable-line functional/immutable-data
    return record;
  }

  // Remove old one
  record.removeField(field);
  // Append new one
  record.insertField({
    tag: field.tag,
    ind1: field.ind1,
    ind2: field.ind2,
    subfields: field.subfields.map(sub => {
      if (sub.code === to.value.code) {
        return {code: to.value.code, value: formatedValue};
      }
      return sub;
    })
  });

  // Return record
  return record;
}
