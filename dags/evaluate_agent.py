"""Parameterized Airflow DAG: run a coding agent on SWE-bench, evaluate it, log to MLflow.

Pipeline: prepare_run -> run_agent -> run_eval -> summarize_and_log

Every experiment value (split, subset, workers, model, task_slice, run_id,
cost_limit) is a DAG param -- nothing is hardcoded. Each run writes a fully
self-describing tree under runs/<run-id>/ that can be reproduced from the folder
alone, and params/metrics/artifact refs are logged to a local MLflow file store.
"""

import hashlib
import json
import os
import subprocess
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param

# ---------------------------------------------------------------------------
# Paths & configuration
# ---------------------------------------------------------------------------
# dags/ lives one level under the project root, same as the starter DAG.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNS_ROOT = PROJECT_ROOT / "runs"

# Local MLflow file store. Single constant -- swap for a sqlite/remote URI later
# (Phase 3) without touching any logging code below.
MLFLOW_TRACKING_URI = f"file:{PROJECT_ROOT / 'mlruns'}"
MLFLOW_EXPERIMENT = "swebench-agent-eval"

# Agent config file shipped with mini-swe-agent (from mini-swe-bench-batch.sh).
AGENT_CONFIG = "mini-swe-agent/src/minisweagent/config/benchmarks/swebench.yaml"

# Maps the (subset, split) params to the HuggingFace dataset the eval harness
# loads. Extend this table if you add subsets/splits.
DATASET_BY_SUBSET = {
    "verified": "princeton-nlp/SWE-bench_Verified",
    "lite": "princeton-nlp/SWE-bench_Lite",
    "full": "princeton-nlp/SWE-bench",
}


def _run_dir(run_id: str) -> Path:
    return RUNS_ROOT / run_id


def _uv_env() -> dict:
    """Environment for subprocesses: inherit everything, keep cost tracking lenient."""
    return {**os.environ, "MSWEA_COST_TRACKING": "ignore_errors"}


@dag(
    dag_id="evaluate_agent",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={
        # ---- required ----
        "split": Param("test", type="string", description="Dataset split, e.g. test"),
        "subset": Param(
            "verified",
            type="string",
            enum=["verified", "lite", "full"],
            description="SWE-bench subset",
        ),
        "workers": Param(5, type="integer", minimum=1, description="Parallel workers for agent + eval"),
        # ---- optional ----
        "model": Param(
            "nebius/moonshotai/Kimi-K2.6",
            type="string",
            description="LiteLLM model string served via Nebius Token Factory",
        ),
        "task_slice": Param(
            "0:3",
            type=["null", "string"],
            description="Instance slice, e.g. '0:3'. Null = whole subset.",
        ),
        "run_id": Param(
            None,
            type=["null", "string"],
            description="Explicit run id. If null, one is generated from the timestamp.",
        ),
        "cost_limit": Param(
            0,
            type="number",
            description="Per-instance cost limit passed to the agent (0 = unlimited/ignored).",
        ),
    },
    tags=["mlops", "swebench", "agent-eval"],
)
def evaluate_agent():

    @task
    def prepare_run(params: dict = None) -> dict:
        """Resolve params, create runs/<run-id>/ tree, freeze config.json."""
        params = params or {}
        run_id = params.get("run_id") or f"run-{datetime.utcnow():%Y%m%d-%H%M%S}"

        run_dir = _run_dir(run_id)
        agent_dir = run_dir / "run-agent"
        eval_dir = run_dir / "run-eval"
        for d in (agent_dir, eval_dir):
            d.mkdir(parents=True, exist_ok=True)

        # Resolved config -- the single source of truth for this run.
        config = {
            "run_id": run_id,
            "split": params["split"],
            "subset": params["subset"],
            "workers": int(params["workers"]),
            "model": params.get("model"),
            "task_slice": params.get("task_slice"),
            "cost_limit": params.get("cost_limit", 0),
            "dataset_name": DATASET_BY_SUBSET.get(params["subset"]),
            "created_at": datetime.utcnow().isoformat() + "Z",
        }
        (run_dir / "config.json").write_text(json.dumps(config, indent=2))
        return config

    @task
    def run_agent(config: dict) -> dict:
        """Run mini-swe-agent in batch over the configured slice.

        Reproduces scripts/mini-swe-bench-batch.sh with parameterized flags and
        output redirected into the run tree. Produces run-agent/preds.json plus
        per-instance trajectories.
        """
        run_dir = _run_dir(config["run_id"])
        agent_out = run_dir / "run-agent"

        cmd = [
            "uv", "run", "mini-extra", "swebench",
            "--subset", config["subset"],
            "--split", config["split"],
            "--model", config["model"],
            "--config", AGENT_CONFIG,
            "--workers", str(config["workers"]),
            "-o", str(agent_out),
        ]
        # Optional flags only when set.
        if config.get("task_slice"):
            cmd += ["--slice", config["task_slice"]]

        log_path = agent_out / "run-agent.log"
        with open(log_path, "w") as log:
            subprocess.run(cmd, cwd=PROJECT_ROOT, env=_uv_env(),
                           check=True, stdout=log, stderr=subprocess.STDOUT)

        preds_path = agent_out / "preds.json"
        if not preds_path.exists():
            raise FileNotFoundError(f"agent did not produce {preds_path}")

        preds = json.loads(preds_path.read_text())
        n_nonempty = sum(1 for v in preds.values() if v.get("model_patch"))
        return {
            "preds_path": str(preds_path),
            "n_instances": len(preds),
            "n_patched": n_nonempty,
        }

    @task
    def run_eval(config: dict, agent_result: dict) -> dict:
        """Run the SWE-bench harness on the agent's predictions.

        Reproduces scripts/swe-bench-eval.sh with parameterized dataset,
        predictions path, workers, and run_id (instead of the hardcoded 'test').
        """
        run_dir = _run_dir(config["run_id"])
        eval_out = run_dir / "run-eval"

        cmd = [
            "uv", "run", "python", "-m", "swebench.harness.run_evaluation",
            "--dataset_name", config["dataset_name"],
            "--split", config["split"],
            "--predictions_path", agent_result["preds_path"],
            "--max_workers", str(config["workers"]),
            "--run_id", config["run_id"],
        ]

        log_path = eval_out / "run-eval.log"
        with open(log_path, "w") as log:
            subprocess.run(cmd, cwd=PROJECT_ROOT, env=_uv_env(),
                           check=True, stdout=log, stderr=subprocess.STDOUT)

        # The harness writes a report json to the project root as
        # <model>.<run_id>.json (dots in model become underscores). Find it and
        # copy it into the run tree so the folder is self-contained.
        report_src = _find_report(config["run_id"])
        report_dst = eval_out / "report.json"
        if report_src is not None:
            report_dst.write_text(report_src.read_text())
        return {"report_path": str(report_dst) if report_src else None}

    @task
    def summarize_and_log(config: dict, agent_result: dict, eval_result: dict) -> dict:
        """Parse the eval report -> metrics.json, write manifest.json, log to MLflow."""
        import mlflow  # imported here so DAG parsing doesn't require mlflow

        run_dir = _run_dir(config["run_id"])

        # ---- metrics from the eval report ----
        metrics = _parse_report(eval_result.get("report_path"))
        metrics.update({
            "n_instances": agent_result["n_instances"],
            "n_patched": agent_result["n_patched"],
        })
        if metrics.get("total_instances"):
            metrics["resolve_rate"] = round(
                metrics["resolved_instances"] / metrics["total_instances"], 4
            )
        (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2))

        # ---- manifest: hash every file for reproducibility/provenance ----
        manifest = _build_manifest(run_dir, config)
        (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))

        # ---- MLflow ----
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment(MLFLOW_EXPERIMENT)
        with mlflow.start_run(run_name=config["run_id"]):
            mlflow.log_params({
                k: config[k] for k in
                ("split", "subset", "workers", "model", "task_slice", "cost_limit", "dataset_name")
            })
            mlflow.log_metrics({k: v for k, v in metrics.items() if isinstance(v, (int, float))})
            # artifact refs -- the whole run tree is the provenance record
            for name in ("config.json", "metrics.json", "manifest.json"):
                p = run_dir / name
                if p.exists():
                    mlflow.log_artifact(str(p))
            mlflow.set_tag("git_sha", manifest.get("git_sha", "unknown"))
        return metrics

    # ---- wiring ----
    cfg = prepare_run()
    agent = run_agent(cfg)
    ev = run_eval(cfg, agent)
    summarize_and_log(cfg, agent, ev)


# ---------------------------------------------------------------------------
# Helpers (module-level so tasks stay readable)
# ---------------------------------------------------------------------------
def _find_report(run_id: str):
    """Locate the SWE-bench evaluation report json for this run_id."""
    candidates = list(PROJECT_ROOT.glob(f"*.{run_id}.json"))
    if not candidates:
        # newer swebench versions may nest it; search a couple of common spots
        candidates = list(PROJECT_ROOT.glob(f"**/*{run_id}*.json"))
    return candidates[0] if candidates else None


def _parse_report(report_path) -> dict:
    """Extract resolved/total counts from a SWE-bench report json.

    SWE-bench reports use keys like total_instances, resolved_instances,
    and lists such as resolved_ids. We defensively handle both count- and
    list-style fields.
    """
    if not report_path or not Path(report_path).exists():
        return {"resolved_instances": 0, "total_instances": 0, "report_found": False}

    data = json.loads(Path(report_path).read_text())

    def _count(count_key, list_key):
        if count_key in data and isinstance(data[count_key], int):
            return data[count_key]
        if list_key in data and isinstance(data[list_key], list):
            return len(data[list_key])
        return 0

    return {
        "report_found": True,
        "total_instances": _count("total_instances", "submitted_ids"),
        "resolved_instances": _count("resolved_instances", "resolved_ids"),
        "unresolved_instances": _count("unresolved_instances", "unresolved_ids"),
        "error_instances": _count("error_instances", "error_ids"),
        "empty_patch_instances": _count("empty_patch_instances", "empty_patch_ids"),
    }


def _build_manifest(run_dir: Path, config: dict) -> dict:
    """Walk the run tree, hash every file, capture git sha -- reproducibility record."""
    files = {}
    for p in sorted(run_dir.rglob("*")):
        if p.is_file() and p.name != "manifest.json":
            h = hashlib.sha256(p.read_bytes()).hexdigest()
            files[str(p.relative_to(run_dir))] = {"sha256": h, "bytes": p.stat().st_size}

    git_sha = "unknown"
    try:
        git_sha = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=PROJECT_ROOT,
            capture_output=True, text=True, check=True,
        ).stdout.strip()
    except Exception:
        pass

    return {
        "run_id": config["run_id"],
        "git_sha": git_sha,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "config": config,
        "files": files,
    }


evaluate_agent()
