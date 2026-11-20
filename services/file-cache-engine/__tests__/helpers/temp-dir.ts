import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

export interface TempDir {
  readonly path: string;
  cleanup(): Promise<void>;
}

export async function makeTempDir(prefix = 'file-cache-'): Promise<TempDir> {
  const path = await mkdtemp(join(tmpdir(), prefix));
  return {
    path,
    cleanup: (): Promise<void> => rm(path, { recursive: true, force: true }),
  };
}
