import { Readable } from 'node:stream';
import { join as joinPath } from 'node:path';
import {
  S3Client,
  ListObjectsCommand,
  GetObjectCommand,
} from '@aws-sdk/client-s3';

import { FileStorage } from './file-storage';

export type S3FileStorageOptions = {
  bucketName: string;
  rootPath: string;
};

export class S3FileStorage implements FileStorage {
  constructor(
    protected readonly s3Client: S3Client,
    protected readonly options: S3FileStorageOptions
  ) {}

  async getAvailableFiles({ path }: { path?: string }): Promise<string[]> {
    const command = new ListObjectsCommand({
      Bucket: this.options.bucketName,
      Prefix: joinPath(this.options.rootPath, path),
    });

    const response = await this.s3Client.send(command);

    // DM: @TODO pagination required

    const files = (response.Contents || []).map((item) => item.Key || '');

    return files;
  }

  async getDownloadStream({
    filePath,
  }: {
    filePath: string;
  }): Promise<Readable> {
    const command = new GetObjectCommand({
      Bucket: this.options.bucketName,
      Key: filePath,
    });

    const response = await this.s3Client.send(command);

    if (!response.Body) {
      throw new Error('Body not found in response');
    }

    return response.Body as Readable;
  }
}
