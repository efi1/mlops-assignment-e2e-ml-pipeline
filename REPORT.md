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

**Reproducing / rerunning by run-id.** Each run's exact inputs are frozen in
`runs/<run-id>/config.json`. To understand a past run, read that folder — config,
predictions, per-instance trajectories, the eval report, metrics, and `manifest.json`
(per-file hashes + git sha) fully describe it, so the folder alone tells the whole
story. A complete sample run is committed under `runs/run-20260704-172339/` (3 instances, 2 resolved) for inspection.
To re-execute, trigger the DAG with the params from that `config.json`: passing
the same `run_id` reuses the existing run folder, while omitting it generates a fresh
run-id with identical settings (useful for a side-by-side comparison in MLflow).
Because the agent uses a sampling-based LLM, the produced patches may differ slightly
between runs — the pipeline and configuration are reproducible, not the exact model
output.

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
MLflow runs table across several runs is captured in `screenshots/mlflow_runs.png`,
and the uploaded run tree in object storage in `screenshots/object_storage_artifacts.png`.

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
- **Nebius authentication.** During development the agent failed with a 401
  AuthenticationError and silently produced empty patches, because the API key never
  reached the agent process. The initial fix was placing the key in
  `~/.config/mini-swe-agent/.env`, which the agent reads directly. Later testing
  showed that in the containerized setup this file isn't needed at all — the key is
  delivered as an environment variable (project `.env` → compose → task containers),
  so setup is just `cp .env.example .env` with the real key.
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
cp .env.example .env      # then edit .env: set your real NEBIUS_API_KEY, local paths, and ports

docker compose up -d      # launches the services on the ports defined in .env
                          # defaults: Airflow 8080, MLflow 5000, MinIO 9000 (API) / 9001 (console)
```

The UIs are reachable via an SSH tunnel from your machine:
`ssh -L 8080:localhost:8080 -L 5000:localhost:5000 -L 9001:localhost:9001 <user>@<vm-ip>`.
Airflow login is `admin` / `admin`.

Trigger `evaluate_agent` with, e.g., `{"split":"test","subset":"verified","workers":1,"task_slice":"0:1"}`.

> **⚠️ API key.** Put your Nebius Token Factory key in the project `.env`
> (`cp .env.example .env`, then edit). Docker Compose reads this file automatically
> and passes the key into the containers as an environment variable — no other setup
> is needed. The `.env` file is gitignored and must never be committed.
> Only when running the agent *outside* Docker (standalone on the host) does it
> additionally need the key in `~/.config/mini-swe-agent/.env`, since the agent reads
> that file itself in host-side runs.

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
tasks can launch sibling containers. See `screenshots/airflow_services_health.png`.
The compose file is parameterized through `.env` — service ports, S3 credentials, and
the Airflow admin login all have sensible defaults and can be overridden there without
editing the compose file itself.

**DockerOperator tasks.** `run_agent` and `run_eval` were converted from direct
subprocess calls to `DockerOperator` tasks that run in the `agent-eval` image, giving
each step an isolated execution environment. The Airflow Task Instances view confirms
their operator type is `DockerOperator` while the lighter steps remain `@task`
(`screenshots/airflow_dag.png`).

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
(`screenshots/docker_ps_hierarchy.png`) shows the three layers: the three
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

## 12. Limitations and possible improvements

This solution works and meets the requirements, but it ended up fairly heavy — there are four layers of Docker (compose services, the Airflow container, the per-step task containers, and the SWE-bench containers). I wanted to note the main things I'd improve:

- **Docker socket mount.** To let the containers start other containers, I mounted the host Docker socket, which effectively gives them root access to the host. It's fine on a single-user course VM, but not something you'd want in a shared environment. The assignment mentions `KubernetesPodOperator` as the better option for real isolation.
- **API key in a file (resolved for the Docker path).** The agent can read its key from `~/.config/mini-swe-agent/.env`, which originally required a manual setup step and caused a confusing failure during development (empty patches with no obvious error). Testing showed the containerized pipeline doesn't need that file at all — the key is passed as an environment variable, from the project `.env` through compose into the task containers. The config file is only needed when running the agent outside Docker.
- **Things resolved at runtime.** Airflow and its providers are installed when the container starts, and the agent config path is found with a subshell inside the command. Baking these into the image would be more robust.
- **Local disk + S3.** Data is written locally and then copied to S3, so the pipeline still depends on the VM's disk. Using object storage as the main storage would decouple it from any single machine.
- **Airflow login (configurable, but defaults to admin/admin).** The admin credentials now come from `.env` (`AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD`), so they can be changed without touching the compose file — but the default in both `.env`.example and the compose fallback is still `admin/admin`, and the value lives in a plaintext .env. Fine for a local single-user VM; a real deployment should force a non-default password and use a proper secrets mechanism.

None of these affected the results here; they're the kind of things I'd want to improve if this were going to run for real.
