import { tmpdir } from 'node:os';
import { join as joinPath, basename } from 'node:path';
import { mkdirSync, createWriteStream } from 'node:fs';

import { S3Client } from '@aws-sdk/client-s3';
import { get as env } from 'env-var';

import { FileStorage } from './lib/file-storage';
import { S3FileStorage } from './lib/s3-file-storage';

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
mkdirSync(TEMP_DIR);

(async () => {
  const files = await fileStorage.getAvailableFiles({
    path: COMPANY_DATA_PATH,
  });

  for (const filePath of files) {
    const stream = await fileStorage.getDownloadStream({ filePath });

    const fileName = basename(filePath);
    const savePath = joinPath(TEMP_DIR, fileName);

    const fileWriteStream = createWriteStream(savePath);
    stream.pipe(fileWriteStream);

    console.log(`Downloaded ${fileName} to ${savePath}`);
  }

  return files;
})()
  .then((_) => console.log(_))
  .catch(console.error);
