// Creates N idle sandboxes, prints how much host memory each one costs, then destroys them.
//   tsx benchmarks/sandbox/hold.ts bowl|e2b <n>
import fs from 'node:fs';
import { e2b } from '@computesdk/e2b';
import { bowl } from './bowl.js';

const [which, count] = [process.argv[2], Number(process.argv[3] ?? 20)];
const compute: any =
  which === 'bowl'
    ? bowl({ url: process.env.BOWL_SANDBOX_URL!, apiKey: process.env.BOWL_SANDBOX_API_KEY, template: process.env.BOWL_TEMPLATE ?? 'node:22-slim' })
    : e2b({ apiKey: process.env.E2B_API_KEY! });
const options = which === 'bowl' ? {} : { templateId: process.env.E2B_TEMPLATE, timeout: 900_000 };
const availableKb = () => Number(/MemAvailable:\s+(\d+)/.exec(fs.readFileSync('/proc/meminfo', 'utf8'))![1]);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// One throwaway sandbox first, so whatever is shared by all of them (page cache of the template) is already counted.
await (await compute.sandbox.create(options)).destroy();
await sleep(3000);
const before = availableKb();
const boxes = [];
for (let i = 0; i < count; i++) boxes.push(await compute.sandbox.create(options));
await sleep(5000);
const after = availableKb();
console.log(`${which}: ${count} idle sandboxes, host MemAvailable dropped ${Math.round((before - after) / count)} kB each`);
for (const box of boxes) await box.destroy();
