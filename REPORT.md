# Report: Automated Coding-Agent Evaluation Pipeline

MLOps assignment — converting an ad-hoc SWE-bench evaluation workflow into a
reproducible, parameterized Airflow pipeline with MLflow tracking.

## 1. Problem and goal

Researchers were running a coding agent against SWE-bench by hand on a VM: no run
history, no provenance, no queue. The goal of this assignment is to productionize
that workflow — turn the manual `scripts/*.sh` steps into a single, configurable
Airflow DAG that produces a fully reproducible artifact tree and logs every run to
MLflow.

The system under evaluation is **mini-swe-agent** (an LLM-driven coding agent). The
pipeline runs the agent over a slice of SWE-bench Verified, evaluates the patches it
produces with the SWE-bench harness, and records the results. The pipeline itself is
the deliverable — not the agent or the harness, both of which are dependencies.

## 2. Architecture

The DAG `evaluate_agent` (in `dags/evaluate_agent.py`) has four tasks wired linearly:

```
prepare_run -> run_agent -> run_eval -> summarize_and_log
```

| Task | Role | Output |
|------|------|--------|
| `prepare_run` | Resolve params, create the run tree, freeze `config.json` | `runs/<run-id>/config.json` |
| `run_agent` | Run mini-swe-agent in batch over the slice | `run-agent/preds.json` + per-instance trajectories |
| `run_eval` | Run the SWE-bench harness on the agent's patches | `run-eval/` report + logs |
| `summarize_and_log` | Parse the report, write metrics + manifest, log to MLflow | `metrics.json`, `manifest.json`, MLflow run |

Two stages do two different jobs. `run_agent` *produces* candidate patches — it does
not judge them. `run_eval` is the evaluator: it applies each patch to a clean repo in
an isolated Docker container and runs the real test suite. An instance is "resolved"
only if all its target tests pass. No LLM is involved in evaluation.

### Environment model

A subtlety that shaped the whole implementation: Airflow runs in its own `uv tool`
environment, while the agent, harness, and MLflow live in the project's `.venv`. The
DAG code is parsed and executed by Airflow's environment, which cannot import the
project's packages. The pipeline therefore crosses into the project venv by shelling
out with `uv run ...` for every step that needs project packages, rather than
importing them at the top of the DAG file.

## 3. Parameterization

Every experiment value is a DAG parameter — nothing is hardcoded.

| Param | Required | Default | Meaning |
|-------|----------|---------|---------|
| `split` | yes | `test` | Dataset split |
| `subset` | yes | `verified` | SWE-bench subset (maps to a HuggingFace dataset) |
| `workers` | yes | 5 | Parallel workers for agent and eval |
| `model` | no | `nebius/moonshotai/Kimi-K2.6` | LiteLLM model string (served via Nebius Token Factory) |
| `task_slice` | no | `0:3` | Instance slice; null = whole subset |
| `run_id` | no | timestamp | Explicit run id; auto-generated if null |
| `cost_limit` | no | 0 | Per-instance cost limit |

## 4. Reproducible artifact tree

Each run writes a self-describing tree that can be reproduced from the folder alone:

```
runs/<run-id>/
├── config.json          # all resolved params (source of truth for the run)
├── run-agent/
│   ├── preds.json        # predicted patches (the agent -> eval handoff)
│   └── <instance_id>/    # per-instance trajectory (full think/act log)
├── run-eval/
│   └── report.json       # SWE-bench evaluation report
├── metrics.json          # resolved / total / resolve_rate + agent-side counts
└── manifest.json         # sha256 of every file + git sha, for provenance
```

`config.json` freezes the inputs; `manifest.json` hashes every output file and
records the git commit, so a run is both reproducible and verifiable.

## 5. MLflow tracking

`summarize_and_log` logs to MLflow under the `swebench-agent-eval` experiment:
params (split, subset, workers, model, slice, cost_limit, dataset), metrics
(resolved / total / resolve_rate / n_patched), the JSON artifacts, and a `git_sha`
tag. The tracking backend is a local SQLite store (`sqlite:///mlflow.db`) — the URI
is a single configurable constant, so it can be pointed at a server later without
changing any logging code.

## 6. Results

Three-instance run (`task_slice: "0:3"`, `workers: 3`) on the first three
SWE-bench Verified astropy instances:

| Metric | Value |
|--------|-------|
| n_instances | 3 |
| n_patched | 3 |
| resolved_instances | 2 |
| unresolved_instances | 1 |
| resolve_rate | 0.667 |

All three instances produced a patch (no empty patches); the harness confirmed two
of them actually fix the bug. The mixed result validates that the pipeline correctly
distinguishes resolved from unresolved — the agent attempts, the harness judges, and
the metrics capture the real outcome. An earlier single-instance smoke run
(`astropy__astropy-12907`) resolved 1/1, confirming the end-to-end flow first. The
MLflow runs table across several runs is captured in `docs/screenshots/mlflow-runs.png`,
and the uploaded run tree in object storage in `docs/screenshots/minio-bucket.png`.

## 7. Notable issues and fixes

- **Agent config path.** The starter script referenced a source-tree path
  (`mini-swe-agent/src/...`) that does not exist when mini-swe-agent is installed as a
  dependency. Resolved by querying the installed package location at runtime via
  `uv run`.
- **Two environments.** `import mlflow` / `import minisweagent` fail at DAG-parse time
  because Airflow's env lacks the project packages. Handled by shelling out with
  `uv run`; mlflow was additionally made available to Airflow via `--with mlflow`.
- **MLflow file store deprecated.** MLflow 3.x rejects the file store; switched the
  tracking URI to SQLite.
- **Nebius authentication.** The agent reads its key from
  `~/.config/mini-swe-agent/.env`. The Token Factory API key must be placed there (a
  key in the project `.env` or shell alone was not picked up). This was the single
  biggest gotcha — a fresh clone must set this file or the agent fails with a 401
  AuthenticationError and produces empty patches.
- **resolve_rate denominator.** The harness report's `total_instances` counts the whole
  dataset (500), not the slice. `resolve_rate` is computed against the actual number of
  instances run (`n_instances`) so the rate is honest.

## 8. Setup (VM)

```bash
sudo apt-get update && sudo apt-get install -y python3-dev git curl
curl -fsSL https://get.docker.com | sh && sudo usermod -aG docker $USER && newgrp docker
curl -LsSf https://astral.sh/uv/install.sh | sh && source $HOME/.local/bin/env
git clone <repo> && cd mlops-assignment-e2e-ml-pipeline
uv sync
mkdir -p ~/.config/mini-swe-agent
echo 'NEBIUS_API_KEY=<token-factory-key>' > ~/.config/mini-swe-agent/.env
./run-airflow-standalone.sh            # UI on localhost:8080 (reach via SSH tunnel)

# local S3 for Phase 2
docker run -d --name minio -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  -v ~/minio-data:/data quay.io/minio/minio server /data --console-address ":9001"
```

Trigger `evaluate_agent` with, e.g., `{"split":"test","subset":"verified","workers":1,"task_slice":"0:1"}`.

## 9. Phase 2 — durability (done)

Two durability mechanisms:

- **Manifest** — `manifest.json` records a sha256 of every file in the run tree plus
  the git sha, so any archived run is both reproducible and verifiable.
- **S3 upload** — a fifth task, `upload_to_s3`, walks the run tree and uploads every
  file to an S3-compatible bucket under a `<run-id>/` prefix, so runs survive VM
  teardown. It uses boto3 and reads endpoint/bucket/region/credentials from
  configuration, so the same code targets local storage now and cloud storage later
  with no change.

Storage backend: a **local MinIO** container (S3-compatible) is used, per course
guidance that provisioning cloud Object Storage requires admin permissions the class
accounts do not have. Because MinIO speaks the S3 protocol, switching to Nebius Object
Storage is purely a matter of changing the endpoint URL and credentials.

Wiring: `upload_to_s3` runs after `summarize_and_log`, so `metrics.json` and
`manifest.json` exist before the tree is uploaded.

Each run is therefore persisted in two places: the local `runs/<run-id>/` tree (for
immediate inspection and reproducibility) and the S3 bucket (for durability beyond the
VM's lifetime). The two hold identical content, keyed by run-id.

## 10. Phase 3 — production (done)

Three production improvements were made.

**docker-compose services.** Airflow, MLflow, and MinIO now run as containers
defined in `docker-compose.yaml`, replacing the standalone launch script. MLflow
runs as a tracking server (`http://mlflow:5000`) and MinIO as S3 storage; the DAG
reaches them by service name over the compose network. Airflow is built from the
provided `Dockerfile` (the `agent-eval` image) and mounts the host Docker socket so
tasks can launch sibling containers. See `docs/screenshots/airflow-services-health.png`.

**DockerOperator tasks.** `run_agent` and `run_eval` were converted from direct
subprocess calls to `DockerOperator` tasks that run in the `agent-eval` image, giving
each step an isolated execution environment. The Airflow Task Instances view confirms
their operator type is `DockerOperator` while the lighter steps remain `@task`
(`docs/screenshots/airflow-dag-dockeroperator-success.png`).

Because `DockerOperator` does not return Python values the way `@task` functions do,
inter-task data flows through the shared run tree on disk rather than XCom: the
containerized steps write `preds.json` and the eval report into `runs/<run-id>/`, and
the downstream Python tasks read those files. The `run_id` and other parameters are
passed into the DockerOperator commands via Jinja templating from `prepare_run`'s
XCom. This is a deliberate, production-realistic choice — real pipelines pass
artifacts through storage, not by returning large objects through the orchestrator.

**Execution model (Docker-outside-of-Docker).** The DockerOperator creates one task
container per step; inside it, mini-swe-agent (or the harness) spawns the per-instance
SWE-bench containers via the mounted socket. All containers are therefore siblings on
the host daemon rather than truly nested. A mid-run `docker ps`
(`docs/screenshots/docker-ps-hierarchy.png`) shows the three layers: the three
long-running compose services, one ephemeral `agent-eval` task container, and the
per-instance SWE-bench containers it spawned. The socket mount is the standard
approach for this pattern; its security trade-off (host Docker access) is acceptable
for a single-tenant course environment.

**Retries and timeouts.** DockerOperator tasks get one retry and a 45-minute
execution timeout; the Python tasks get two retries via `default_args`.

**Side benefit — clean working directory.** In Phases 1–2 the SWE-bench harness ran
with its working directory at the project root, so it left a redundant report copy
(`<model>.<run-id>.json`) there after every run. In Phase 3 the harness runs inside
the task container with its working directory set to `runs/<run-id>/run-eval/`, so the
report is written directly into the run tree and nothing is left at the project root.

## 11. Phase progression (design note)

The pipeline was built in the order the assignment prescribes: a working Python/Bash
DAG first (Phase 1), then durability (Phase 2), then production isolation (Phase 3).
The commit history reflects this arc. Building the simple version first, proving it
end to end, and only then hardening it into containers was intentional — it isolated
failures at each stage (the containerized run in Phase 3 was far easier to debug
because the pipeline logic was already known-good) and mirrors how such a system would
be developed in practice.
