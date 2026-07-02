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

## 6. Results (smoke run)

Single-instance run (`task_slice: "0:1"`) on `astropy__astropy-12907`:

| Metric | Value |
|--------|-------|
| Exit status | Submitted |
| n_instances | 1 |
| n_patched | 1 |
| resolved_instances | 1 |
| resolve_rate | 1.0 |

The agent produced a valid patch and the harness confirmed it fixes the bug (the
target tests pass). This validated the full pipeline end to end on real cloud infra.

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
```

Trigger `evaluate_agent` with, e.g., `{"split":"test","subset":"verified","workers":1,"task_slice":"0:1"}`.

## 9. Phase 2 — durability (planned)

Manifest with file hashes + git sha is already implemented. Remaining: upload the run
tree to S3-compatible object storage so runs survive VM teardown, and redirect the
harness report directly into the run folder rather than the project root.

## 10. Phase 3 — production (planned)

Move Airflow and MLflow into `docker-compose` as services, switch `run_agent` /
`run_eval` to `DockerOperator` for execution isolation, and add retries/timeouts.
Screenshots of the Airflow graph and MLflow run to be included.
