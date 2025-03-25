import { Readable } from 'node:stream';

export interface FileStorage {
  getAvailableFiles: ({}: { path: string }) => Promise<string[]>;

  getDownloadStream: ({}: { filePath: string }) => Promise<Readable>;
}
