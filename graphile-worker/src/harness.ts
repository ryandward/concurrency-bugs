import { EventEmitter } from 'node:events';
import pg from 'pg';
import { Logger, makeWorkerUtils, type RunnerOptions, type WorkerEvents, type WorkerUtils } from 'graphile-worker';
import type { Adapter } from './adapters.ts';

/** The harness's own schema, so pointing it at a shared database touches nothing else. */
export const SCHEMA = 'gw_race';
const TASK = 'race';
/** How long the job's outcome write stays blocked behind our row lock. */
const HOLD_MS = 500;
/** How long we keep watching after the lock is released, for late writes, retries and rejections. */
const SETTLE_MS = 1500;

/** What an app reading the public jobs view would conclude about the job. */
type JobState = 'gone' | 'running' | 'queued' | 'retrying' | 'spent';

export interface CaseResult {
  readonly ok: boolean;
  readonly saw: string;
}

export interface World {
  readonly url: string;
  readonly pool: pg.Pool;
  /** Fixtures and observation only; the worker never uses it. */
  readonly admin: pg.Client;
  readonly utils: WorkerUtils;
}

const quiet = new Logger(() => () => undefined);
const logger = process.env.VERBOSE ? undefined : quiet;

export async function openWorld(url: string): Promise<World> {
  const pool = new pg.Pool({ connectionString: url });
  pool.on('error', () => undefined);
  pool.on('connect', (client) => client.on('error', () => undefined));
  const admin = new pg.Client({ connectionString: url });
  await admin.connect();
  const utils = await makeWorkerUtils({ pgPool: pool, schema: SCHEMA, logger });
  await utils.migrate();
  return { url, pool, admin, utils };
}

export async function closeWorld(world: World): Promise<void> {
  await world.utils.release();
  await world.admin.end();
  await world.pool.end();
}

/** Late rejections land in whichever case is running; SETTLE_MS keeps them from leaking. */
let unhandledSink: unknown[] = [];
process.on('unhandledRejection', (reason) => {
  unhandledSink.push(reason);
});

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));
const describeError = (e: unknown): string => (e instanceof Error ? e.message : String(e));

/** A FOR UPDATE on the job's row from our own connection, which the worker's outcome write queues behind. */
class RowLock {
  private client: pg.Client | null = null;
  private releasing: Promise<void> | null = null;
  released = false;
  private readonly url: string;

  constructor(url: string) {
    this.url = url;
  }

  async take(jobId: string): Promise<void> {
    const client = new pg.Client({ connectionString: this.url });
    this.client = client;
    await client.connect();
    await client.query('begin');
    await client.query(`select 1 from ${SCHEMA}._private_jobs where id = $1 for update`, [jobId]);
    setTimeout(() => void this.release(), HOLD_MS);
  }

  release(): Promise<void> {
    this.releasing ??= (async () => {
      this.released = true;
      if (this.client) {
        await this.client.query('commit');
        await this.client.end();
      }
    })();
    return this.releasing;
  }
}

interface Probe {
  readonly lock: RowLock;
  readonly events: WorkerEvents;
  readonly fatal: string[];
  readonly unhandled: unknown[];
  readonly jobId: string;
  readonly options: RunnerOptions;
  readonly state: () => Promise<JobState>;
}

interface Stage {
  /** The task throws once it holds the lock, so the outcome write is failJob's. */
  readonly taskFails?: boolean;
  /** Make the completion DELETE raise this SQLSTATE, this many times. */
  readonly failDelete?: { readonly code: string; readonly times: number };
}

async function stage(world: World, spec: Stage): Promise<Probe> {
  const { admin } = world;
  await admin.query(`drop trigger if exists race_fail on ${SCHEMA}._private_jobs`);
  await admin.query(`drop function if exists ${SCHEMA}.race_fail()`);
  await admin.query(`drop sequence if exists ${SCHEMA}.race_fail_n`);
  await admin.query(`delete from ${SCHEMA}._private_jobs`);
  if (spec.failDelete) {
    const { code, times } = spec.failDelete;
    await admin.query(`create sequence ${SCHEMA}.race_fail_n`);
    await admin.query(`create function ${SCHEMA}.race_fail() returns trigger language plpgsql as $$
      begin
        if nextval('${SCHEMA}.race_fail_n') <= ${String(times)} then
          raise exception 'injected outcome-write failure' using errcode = '${code}';
        end if;
        return old;
      end $$`);
    await admin.query(`create trigger race_fail before delete on ${SCHEMA}._private_jobs
      for each row execute function ${SCHEMA}.race_fail()`);
  }

  const job = await world.utils.addJob(TASK, {}, { maxAttempts: 3 });
  const lock = new RowLock(world.url);
  const events: WorkerEvents = new EventEmitter();
  const fatal: string[] = [];
  events.on('pool:fatalError', ({ action, error }) => fatal.push(`${action}: ${describeError(error)}`));
  unhandledSink = [];

  const options: RunnerOptions = {
    pgPool: world.pool,
    schema: SCHEMA,
    events,
    logger,
    noHandleSignals: true,
    concurrency: 1,
    taskList: {
      [TASK]: async (_payload, helpers) => {
        await lock.take(helpers.job.id);
        if (spec.taskFails) throw new Error('task failed on purpose');
      },
    },
  };

  const state = async (): Promise<JobState> => {
    const { rows } = await admin.query<{ attempts: number; max_attempts: number; lock_fresh: boolean }>(
      `select attempts, max_attempts,
              coalesce(locked_at > now() - interval '4 hours', false) as lock_fresh
         from ${SCHEMA}.jobs where id = $1`,
      [job.id],
    );
    const row = rows[0];
    if (row === undefined) return 'gone';
    if (row.lock_fresh) return 'running';
    if (row.attempts === 0) return 'queued';
    if (row.attempts < row.max_attempts) return 'retrying';
    return 'spent';
  };

  return { lock, events, fatal, unhandled: unhandledSink, jobId: job.id, options, state };
}

async function settle(probe: Probe): Promise<JobState> {
  await probe.lock.release();
  await sleep(SETTLE_MS);
  return probe.state();
}

/** How a call ended, without letting a rejection escape the case. */
async function outcomeOf(call: Promise<void>): Promise<string> {
  try {
    await call;
    return 'resolved';
  } catch (e) {
    return `rejected (${describeError(e)})`;
  }
}

function blocked(probe: Probe): string {
  return probe.lock.released ? '' : ' while its write was still blocked';
}

function aftermath(probe: Probe): string {
  const parts = [
    probe.unhandled.length > 0 ? `${String(probe.unhandled.length)} unhandled rejection(s), which crash a default node process` : '',
    probe.fatal.length > 0 ? `pool:fatalError [${probe.fatal.join('; ')}]` : '',
  ].filter((part) => part !== '');
  return parts.length > 0 ? `; ${parts.join('; ')}` : '';
}

export interface Case {
  readonly name: string;
  readonly run: (adapter: Adapter, world: World) => Promise<CaseResult>;
}

/** The case that started this: a test read a failed job as still running right after runOnce(). */
const failedOutcome: Case = {
  name: "runOnce() returns after a failed job's retry is recorded",
  run: async (adapter, world) => {
    const probe = await stage(world, { taskFails: true });
    const outcome = await outcomeOf(adapter.runOnce(probe.options));
    const atReturn = await probe.state();
    const where = blocked(probe);
    const later = await settle(probe);
    return {
      ok: outcome === 'resolved' && atReturn === 'retrying',
      saw: `${outcome}${where}; job ${atReturn} at return, ${later} after settle${aftermath(probe)}`,
    };
  },
};

const completedOutcome: Case = {
  name: "runOnce() returns after a finished job's row is deleted",
  run: async (adapter, world) => {
    const probe = await stage(world, {});
    const outcome = await outcomeOf(adapter.runOnce(probe.options));
    const atReturn = await probe.state();
    const where = blocked(probe);
    const later = await settle(probe);
    return {
      ok: outcome === 'resolved' && atReturn === 'gone',
      saw: `${outcome}${where}; job ${atReturn} at return, ${later} after settle${aftermath(probe)}`,
    };
  },
};

const stopOutcome: Case = {
  name: "stop() returns after a finished job's row is deleted",
  run: async (adapter, world) => {
    const probe = await stage(world, {});
    const finished = new Promise<void>((resolve) => probe.events.once('job:success', () => resolve()));
    const runner = await adapter.run(probe.options);
    const ended = outcomeOf(runner.promise);
    await finished;
    const outcome = await outcomeOf(runner.stop());
    const atReturn = await probe.state();
    const where = blocked(probe);
    const later = await settle(probe);
    return {
      ok: outcome === 'resolved' && atReturn === 'gone',
      saw: `stop ${outcome}${where}; job ${atReturn} at return, ${later} after settle; runner.promise ${await ended}${aftermath(probe)}`,
    };
  },
};

const completeEvent: Case = {
  name: 'job:complete fires after the outcome is written',
  run: async (adapter, world) => {
    const probe = await stage(world, {});
    let fired = 'never fired';
    probe.events.on('job:complete', () => {
      fired = probe.lock.released ? 'fired after the write was unblocked' : 'fired while the write was still blocked';
    });
    const outcome = await outcomeOf(adapter.runOnce(probe.options));
    const later = await settle(probe);
    return {
      ok: fired === 'fired after the write was unblocked',
      saw: `${fired}; runOnce ${outcome}; job ${later} after settle${aftermath(probe)}`,
    };
  },
};

const failedWrite: Case = {
  name: 'a failed outcome write rejects runOnce()',
  run: async (adapter, world) => {
    const probe = await stage(world, { failDelete: { code: 'P0001', times: Number.MAX_SAFE_INTEGER } });
    const outcome = await outcomeOf(adapter.runOnce(probe.options));
    const where = blocked(probe);
    const later = await settle(probe);
    return {
      ok: outcome.startsWith('rejected') && probe.unhandled.length === 0,
      saw: `runOnce ${outcome}${where}; job ${later} after settle${aftermath(probe)}`,
    };
  },
};

const retriedWrite: Case = {
  name: 'a retryable outcome-write error (40001, once) is retried before runOnce() returns',
  run: async (adapter, world) => {
    const probe = await stage(world, { failDelete: { code: '40001', times: 1 } });
    const outcome = await outcomeOf(adapter.runOnce(probe.options));
    const atReturn = await probe.state();
    const where = blocked(probe);
    const later = await settle(probe);
    return {
      ok: outcome === 'resolved' && atReturn === 'gone' && probe.unhandled.length === 0,
      saw: `runOnce ${outcome}${where}; job ${atReturn} at return, ${later} after settle${aftermath(probe)}`,
    };
  },
};

export const cases: readonly Case[] = [failedOutcome, completedOutcome, stopOutcome, completeEvent, failedWrite, retriedWrite];
