# graphile-worker: outcome writes nobody waits for

When a job finishes, graphile-worker fires the write that records its outcome (the `DELETE` on success, the `UPDATE` on failure) without awaiting it. So `runOnce()` and `stop()` can return, and `job:complete` can fire, before the database knows what happened. If that write fails, nothing catches it.

I hit this as a flaky test: it ran a failing job with `runOnce()`, read the public `jobs` view right after, and saw the job as still running instead of waiting to retry. It failed 6 out of 10 runs.

This harness makes it fail every time. The task takes a `FOR UPDATE` lock on its own job row, so the outcome write has to queue behind it for 500ms. Then it checks what an app reading the `jobs` view would see.

## Run it

```
npm ci
npm run db:up      # throwaway postgres:16 on localhost:55433
npm run race       # ~22s, exits 1 if any guarantee breaks
npm run db:down
```

- `npm run race -- raw` or `npm run race -- batched` runs one adapter
- `ONLY=failed npm run race` runs only the cases whose name contains "failed"
- `VERBOSE=1` turns graphile's own logging back on
- `DATABASE_URL=...` points it at another database. It only touches its own `gw_race` schema.
- `npm run typecheck`

## What it shows

graphile-worker 0.17.3, Node 24, PostgreSQL 16. Same results every run.

| Case | `raw` (batch delays -1, the default) | `batched` (batch delays 0) |
|---|---|---|
| `runOnce()` returns after a failed job's retry is recorded | FAIL | PASS |
| `runOnce()` returns after a finished job's row is deleted | FAIL | PASS |
| `stop()` returns after the row is deleted | FAIL | PASS |
| `job:complete` fires after the write | FAIL | FAIL |
| A failed outcome write rejects `runOnce()` | FAIL: unhandled rejection | FAIL: `pool:fatalError`, job left locked |
| A retryable `40001` is retried before `runOnce()` returns | FAIL: retried, but after it returns | FAIL: never retried, job left locked |

Setting both batch delays to 0 makes `runOnce()` and `stop()` wait, but it trades the crash for a job that stays locked for 4 hours and then runs again, and the batched path never retries anything.

## Plug in your own

`src/adapters.ts` has the two adapters. Add one that provides `runOnce(options)` and `run(options)` returning `{ stop, promise }`, register it, and run `npm run race -- <name>`. The harness only judges what the database and the events show afterwards.

Heads up: graphile never hands you the write's promise, so a wrapper around `runOnce()` can't just await it.

## Where it comes from

- `src/worker.ts`: `completeJob(job)` / `failJob(...)` are called without `await`, and `job:complete` is emitted right after
- `src/main.ts`: at the default delay of -1, the unbatched path has `release: null`, so `terminate()` has nothing to wait for
- `src/main.ts`, `batch()`: the retry loop exits after the first failure
- `src/interfaces.ts`: `CompleteJobFunction` / `FailJobFunction` are typed `=> void`, so nothing flags the dropped promise

The `await`s went away in c542aa3, which shipped in 0.17.0. 0.18.0 and `main` have the same code paths.

## Upstream

- https://github.com/graphile/worker/issues/641 - unhandled rejection on the unbatched path
- https://github.com/graphile/worker/pull/643 - handles that error, doesn't wait for the write
- https://github.com/graphile/worker/issues/635 - `runner.promise` resolves when the pool dies
- https://github.com/graphile/worker/issues/636 - batched retries never run
- https://github.com/graphile/worker/pull/645 - fixes the retry loop
