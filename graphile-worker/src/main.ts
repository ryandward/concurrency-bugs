import { adapters } from './adapters.ts';
import { cases, closeWorld, openWorld, SCHEMA } from './harness.ts';

const url = process.env.DATABASE_URL ?? 'postgres://postgres:pw@localhost:55433/postgres';
const only = process.env.ONLY;
const asked = process.argv.slice(2);
const names = asked.length > 0 ? asked : Object.keys(adapters);
const unknown = names.filter((n) => adapters[n] === undefined);
if (unknown.length > 0) {
  console.error(`unknown adapter(s): ${unknown.join(', ')}; known: ${Object.keys(adapters).join(', ')}`);
  process.exit(2);
}

const world = await openWorld(url);
let failures = 0;
try {
  console.log(`database ${url.replace(/:[^:@/]*@/, ':***@')}, schema ${SCHEMA}\n`);
  for (const name of names) {
    const adapter = adapters[name];
    if (adapter === undefined) continue;
    console.log(`${adapter.name} (${adapter.describe})`);
    for (const c of cases.filter((c) => only === undefined || c.name.includes(only))) {
      const result = await c.run(adapter, world);
      if (!result.ok) failures += 1;
      console.log(`  ${result.ok ? 'PASS' : 'FAIL'}  ${c.name}\n        ${result.saw}`);
    }
    console.log('');
  }
} finally {
  await closeWorld(world);
}
console.log(failures === 0 ? 'all guarantees held' : `${String(failures)} guarantee(s) broken`);
process.exitCode = failures === 0 ? 0 : 1;
