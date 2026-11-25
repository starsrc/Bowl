// Runs the benchmark repo's in-sandbox workloads (CPU hashing, SQLite file I/O) in one sandbox
// and prints their timings. Works against bowl-sandbox and against anything E2B-compatible.
//
//   tsx benchmarks/sandbox/workloads.ts bowl
//   E2B_API_URL=... E2B_API_KEY=... E2B_TEMPLATE=... tsx benchmarks/sandbox/workloads.ts e2b
import fs from 'node:fs';
import path from 'node:path';
import { e2b } from '@computesdk/e2b';
import { bowl } from './bowl.js';

const which = process.argv[2];
const scripts = path.resolve(import.meta.dirname, '../stealth/scripts');
const read = (name: string) => fs.readFileSync(path.join(scripts, name), 'utf8');

const compute: any =
  which === 'bowl'
    ? bowl({ url: process.env.BOWL_SANDBOX_URL!, apiKey: process.env.BOWL_SANDBOX_API_KEY, template: process.env.BOWL_TEMPLATE ?? 'node:22-slim' })
    : e2b({ apiKey: process.env.E2B_API_KEY! });
const options = which === 'bowl' ? {} : { templateId: process.env.E2B_TEMPLATE, timeout: 900_000 };

const t0 = performance.now();
const sandbox = await compute.sandbox.create(options);
console.log(`create_ms\t${Math.round(performance.now() - t0)}`);
try {
  const run = async (cmd: string) => {
    const r = await sandbox.runCommand(cmd, { timeout: 600_000 });
    if (r.exitCode !== 0) throw new Error(`${cmd}: exit ${r.exitCode}: ${r.stderr}`);
    return r.stdout as string;
  };
  await run('mkdir -p /tmp/bench');
  await sandbox.filesystem.writeFile('/tmp/bench/cpu.sh', read('cpu-benchmark.sh'));
  await sandbox.filesystem.writeFile('/tmp/bench/fs-io-workload.js', read('fs-io-workload.js'));
  await sandbox.filesystem.writeFile('/tmp/bench/stdout.js', read('fs-io-stdout.js'));

  console.log((await run('nproc; grep MemTotal /proc/meminfo; uname -r; df -T /tmp | tail -1')).trim().replace(/\n/g, ' | '));
  const cpu = await run('BENCH_SINGLE_MB=1024 BENCH_CHUNK_MB=1024 bash /tmp/bench/cpu.sh');
  for (const line of cpu.split('\n')) if (line.startsWith('BENCH_PHASE')) console.log(`cpu_${line.split('\t')[1]}_ms\t${line.split('\t')[2]}`);

  for (const mode of ['default', 'fsync', 'large']) {
    const out = await run(`BENCH_SUITE=fs-io BENCH_MODE=${mode} node /tmp/bench/fs-io-workload.js`);
    const result = JSON.parse(out.trim().split('\n').at(-1)!);
    console.log(`fsio_${mode}_ms\t${result.ok ? Math.round(result.metric.value) : `FAILED ${result.reason} ${result.error ?? ''}`}`);
  }
} finally {
  await sandbox.destroy();
}
