import { run, runOnce, type RunnerOptions } from 'graphile-worker';

/**
 * One way of driving graphile-worker. Every adapter gets the same options from the harness, and the
 * harness judges it only by what the database and the events show afterwards, so a wrapper (Effect
 * or anything else) passes by actually delivering the guarantee, not by changing what is measured.
 */
export interface Adapter {
  readonly name: string;
  readonly describe: string;
  readonly runOnce: (options: RunnerOptions) => Promise<void>;
  readonly run: (options: RunnerOptions) => Promise<Started>;
}

export interface Started {
  readonly stop: () => Promise<void>;
  readonly promise: Promise<void>;
}

const withBatchDelay = (options: RunnerOptions, delay: number): RunnerOptions => ({
  ...options,
  preset: {
    ...options.preset,
    worker: { ...options.preset?.worker, completeJobBatchDelay: delay, failJobBatchDelay: delay },
  },
});

const raw: Adapter = {
  name: 'raw',
  describe: 'graphile defaults, batch delays -1',
  runOnce: (options) => runOnce(options),
  run: (options) => run(options),
};

const batched: Adapter = {
  name: 'batched',
  describe: 'batch delays 0',
  runOnce: (options) => runOnce(withBatchDelay(options, 0)),
  run: (options) => run(withBatchDelay(options, 0)),
};

/** Add yours here, then `npm run race -- <name>`. */
export const adapters: Readonly<Record<string, Adapter>> = { raw, batched };
