import { readFileSync } from 'node:fs';

import * as csv from 'csv';

export type FinancialRecord = {
  id: string;
  scale: number;

  startDate: Date;
  endDate: Date;

  entries: Array<{ date: Date; value: number }>;
};

export class Loader {
  constructor(protected readonly) {}

  protected parseFile(csvFilePath: string): Promise<Array<FinancialRecord>> {
    return new Promise<Array<FinancialRecord>>((resolve, reject) => {
      const output: Array<FinancialRecord> = [];

      const fileContent = readFileSync(csvFilePath, 'utf-8');

      csv.parse(fileContent, { columns: true }, (err, records) => {
        if (err) {
          return reject(err);
        }

        for (const record of records) {
          const { id, scale, ...rawData } = record;

          const entries = Object.entries(rawData).map(([date, value]) => ({
            date: new Date(date),
            value: Number(value),
          }));

          const startDate = entries[0].date;
          const endDate = entries[entries.length - 1].date;

          console.log('r', record);

          output.push({
            id,
            scale: Number(scale),

            startDate,
            endDate,
            entries,
          });
        }

        return resolve(output);
      });
    });
  }

  async parseFiles(
    ...csvFilePaths: Array<string>
  ): Promise<Array<FinancialRecord>> {
    const output: Array<FinancialRecord> = [];

    for (const csvFilePath of csvFilePaths) {
      output.push(...(await this.parseFile(csvFilePath)));
    }

    return output;
  }
}
