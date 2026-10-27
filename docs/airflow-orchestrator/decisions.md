# Design decisions

Short records: context, decision, what it costs.

## 1. Heavy compute on Cloud Run, not on Airflow workers

**Context.** Trimming, concatenating and encoding video needs GPUs and a lot of memory. Airflow workers are shared by every DAG and sized for orchestration.

**Decision.** Airflow only orchestrates. ffmpeg, ffprobe and transcription run as Cloud Run services called over HTTP. Tasks pass GCS paths, never bytes.

**Cost.** Cold-start latency and a hard per-request timeout. Long encodes must fit inside one request, or be split.

## 2. GPU concurrency = 1, gated by an Airflow pool

**Context.** One GPU instance handles one encode at a time. Without a gate, parallel tasks pile onto the service and get rejected.

**Decision.** The GPU service runs with concurrency 1. Every task that calls it takes a slot in a dedicated Airflow pool, with a separate pool for CPU encodes. Concat tasks get a higher absolute `priority_weight` than trim tasks, so work close to delivery goes first.

**Cost.** Jobs queue during spikes. Total throughput is capped by the pool size, not by demand.

## 3. PostgreSQL as the source of truth, not XCom

**Context.** Jobs get retried, cleared by hand and reprocessed. State that lives only inside a DAG run disappears with it and cannot be queried across jobs.

**Decision.** Job, per-file specs, per-shotcode status and pipeline errors live in PostgreSQL. Every write is an `INSERT … ON CONFLICT` upsert on a natural key. A reprocess bumps a round counter instead of adding rows, and counters are recomputed rather than incremented.

**Cost.** Every write path has to be idempotent, and schema changes must ship together with the code that uses them.

## 4. Selective retries: 429 and 502/503 only

**Context.** A 429 from the GPU service means "busy". A 502/503 usually means an instance is being recycled. Any other 4xx means the request is wrong. A timeout means the server may still be encoding.

**Decision.** The HTTP client retries 429 with a long backoff and its own attempt counter, and 502/503 with a short backoff. It never retries other 4xx or timeouts. Re-sending an encode after a timeout would duplicate heavy work. Airflow task retries sit on top, and mapped tasks use a fixed delay.

**Cost.** A timed-out encode fails the task. Recovery relies on idempotency: an existing proxy in GCS is detected and the encode is skipped.

## 5. Ordered deploys: plugins before DAGs, everything together

**Context.** DAGs import shared plugin code. If a DAG lands before the plugin it depends on, the scheduler fails to import it. A change that touches a DAG, a plugin and a Cloud Run service is only correct when all three are live.

**Decision.** The CD workflow syncs plugins first, then DAGs, then verifies the bucket against the repo and reports missing or extra files. It runs as a dry-run by default, and apply sits behind an approval gate. A partial deploy counts as broken.

**Cost.** Deploys are slower and need a human approval.
