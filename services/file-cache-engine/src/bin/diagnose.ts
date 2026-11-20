#!/usr/bin/env node
import { runCli } from '../diagnostics/cli';

void (async () => {
  const { exitCode, output } = await runCli(process.argv.slice(2));
  console.log(output);
  process.exitCode = exitCode;
})();
