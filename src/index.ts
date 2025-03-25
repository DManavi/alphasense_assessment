import { tmpdir } from 'node:os';
import { join as joinPath, basename } from 'node:path';
import { mkdirSync, createWriteStream } from 'node:fs';
import { globSync } from 'glob';

import { S3Client } from '@aws-sdk/client-s3';
import { get as env } from 'env-var';
import decompress from 'decompress';

import { FileStorage } from './lib/file-storage';
import { S3FileStorage } from './lib/s3-file-storage';
import { Loader } from './lib/loader';
import { Filter } from './lib/filter';

const s3Client = new S3Client({
  region: env('AWS_REGION').required().asString(),
});

const fileStorage: FileStorage = new S3FileStorage(s3Client, {
  bucketName: env('APP_BUCKET_NAME').required().asString(),
  rootPath: '', // root of the bucket
});

const COMPANY_DATA_PATH = env('APP_COMPANY_DATA_PATH').required().asString();

// create temporary directory
const RUN_ID = new Date().valueOf();
const TEMP_DIR = joinPath(tmpdir(), `alphasense-assessment-${RUN_ID}`);
const DIST_DIR = joinPath(TEMP_DIR, 'dist');

// create directories (if not exists)
[TEMP_DIR, DIST_DIR].forEach((dir) => mkdirSync(dir, { recursive: true }));

(async () => {
  // list files in the company data path
  const files = await fileStorage.getAvailableFiles({
    path: COMPANY_DATA_PATH,
  });

  // download each file
  for (const filePath of files) {
    const downloadStream = await fileStorage.getDownloadStream({ filePath });

    const fileName = basename(filePath);
    const savePath = joinPath(TEMP_DIR, fileName);

    const fileWriteStream = createWriteStream(savePath);
    downloadStream.pipe(fileWriteStream);

    // wait for the file to be downloaded (stream to finish)
    await new Promise((resolve, reject) => {
      fileWriteStream.on('finish', resolve);
      fileWriteStream.on('error', reject);
    });

    console.log(`Downloaded ${fileName} to ${savePath}`);
  }

  // list downloaded files
  const zipFiles = globSync('*.zip', { cwd: TEMP_DIR });

  // extract each zip file
  for (const zipFile of zipFiles) {
    await decompress(joinPath(TEMP_DIR, zipFile), DIST_DIR);
  }

  // list extracted files
  const csvFilesPath = globSync('*.csv', { cwd: DIST_DIR }).map((file) =>
    // return full path
    joinPath(DIST_DIR, file)
  );

  // load CSV files and apply filters
  const loader = new Loader();

  // load files (parse CSV)
  const records = await loader.parseFiles(...csvFilesPath);

  const filter = new Filter(records);

  // Return the value of the entry in row "MO_BS_INV" and with a start date of 2014-10-01 in file "MNZIRS0108.csv".
  const result1 = filter.findRow({
    id: 'MO_BS_INV',
    fileName: 'MNZIRS0108.csv',

    date: new Date('2014-10-01'),
  });

  // console.log('Loaded records:', records);
  console.log('Result 1:', result1);
})()
  .then((_) => console.log(_))
  .catch(console.error);
