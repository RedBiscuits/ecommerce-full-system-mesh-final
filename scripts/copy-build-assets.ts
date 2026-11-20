/**
 * `tsc` only emits JavaScript; the services read their SQL schemas next to the
 * compiled code (path.join(__dirname, 'schema.sql')). Copy them into dist/.
 */
import fs from 'node:fs';
import path from 'node:path';

const root = path.resolve(__dirname, '..');
const outDir = path.join(root, 'dist');

function copySqlFiles(dir: string): number {
  let copied = 0;
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    if (entry.name === 'node_modules' || entry.name === 'dist' || entry.name.startsWith('.')) continue;
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      copied += copySqlFiles(full);
    } else if (entry.name.endsWith('.sql')) {
      const target = path.join(outDir, path.relative(root, full));
      fs.mkdirSync(path.dirname(target), { recursive: true });
      fs.copyFileSync(full, target);
      copied += 1;
    }
  }
  return copied;
}

const copied = copySqlFiles(path.join(root, 'services'));
console.log(`copied ${copied} SQL schema file(s) into dist/`);
