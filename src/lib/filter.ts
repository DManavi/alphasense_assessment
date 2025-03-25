import moment from 'moment';

import { FinancialRecord } from './loader';

export class Filter {
  constructor(protected readonly records: Array<FinancialRecord>) {}

  // Return the value of the entry in row "MO_BS_INV" and with a start date of 2014-10-01 in file "MNZIRS0108.csv".

  findRow({
    id,
    date,
    fileName,
  }: {
    id: string;
    date: Date;
    fileName: string;
  }): { record: FinancialRecord; value: number } | undefined {
    const record = this.records.find(
      (_) => _.id === id && _.filename === fileName
    );

    if (typeof record === 'undefined') {
      return undefined;
    }

    const entry = record.entries.find((_) => {
      // find the range
      const rangeStart = moment(_.date);
      const rangeEnd = rangeStart.clone().add(3, 'months');

      return rangeStart.isBefore(date, 'day') && rangeEnd.isAfter(date, 'day');
    });

    if (typeof entry === 'undefined') {
      return undefined;
    }

    return { record, value: entry.value };
  }
}
