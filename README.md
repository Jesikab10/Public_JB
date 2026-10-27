# Public_JB

Architecture write-ups of production systems I designed and built. The source code is private, so this repo documents how each system works and why it is built that way. It contains no secrets, resource IDs, client data or business metrics.

## Contents

| System | Docs |
|---|---|
| **Video processing orchestrator.** Airflow on Cloud Composer, Cloud Run (GPU/CPU ffmpeg), GCS and PostgreSQL. Ingests raw footage, validates it, trims and assembles it from an editorial timeline, and publishes the deliverables. | [Architecture overview](docs/airflow-orchestrator/ARCHITECTURE.md) · [Design decisions](docs/airflow-orchestrator/decisions.md) |

## Runnable counterpart

[media-jobs-mcp](https://github.com/Jesikab10/media-jobs-mcp) is a public, runnable project that applies the same patterns: selective retries, idempotent writes, and explicit job states.
