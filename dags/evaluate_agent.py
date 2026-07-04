"""Parameterized Airflow DAG: run a coding agent on SWE-bench, evaluate it, log to MLflow.

Pipeline: prepare_run -> run_agent -> run_eval -> summarize_and_log -> upload_to_s3

Phase 3 (production): run_agent and run_eval run as DockerOperator tasks in the
provided agent-eval image, for execution isolation. Data flows between tasks
through the shared run tree on disk (runs/<run-id>/), not via XCom objects --
the containerized steps write files, the Python steps read them. run_id is
passed to the DockerOperators via Jinja templating from prepare_run's XCom.

Every experiment value (split, subset, workers, model, task_slice, run_id,
cost_limit) is a DAG param -- nothing is hardcoded.
"""

import hashlib
import json
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# ---------------------------------------------------------------------------
# Paths & configuration
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNS_ROOT = PROJECT_ROOT / "runs"

MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", f"sqlite:///{PROJECT_ROOT / 'mlflow.db'}")
MLFLOW_EXPERIMENT = "swebench-agent-eval"

# ---- S3 / object storage ----
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000")
S3_BUCKET = os.environ.get("S3_BUCKET", "agent-eval-runs")
S3_REGION = os.environ.get("S3_REGION", "us-east-1")

# ---- Docker execution (Phase 3) ----
# HOST_PROJECT_ROOT is the repo path ON THE HOST -- required because DockerOperator
# talks to the host daemon, so mount sources must be host paths.
TASK_IMAGE = os.environ.get("TASK_IMAGE", "agent-eval:latest")
HOST_PROJECT_ROOT = os.environ.get("HOST_PROJECT_ROOT", str(PROJECT_ROOT))
HOST_KEY_DIR = os.environ.get("HOST_KEY_DIR", "/home/efov/.config/mini-swe-agent")
DOCKER_URL = os.environ.get("DOCKER_URL", "unix://var/run/docker.sock")
# Path the run tree is mounted to inside task containers (matches -o paths below).
CONTAINER_PROJECT = "/mlops-assignment"

DATASET_BY_SUBSET = {
    "verified": "princeton-nlp/SWE-bench_Verified",
    "lite": "princeton-nlp/SWE-bench_Lite",
    "full": "princeton-nlp/SWE-bench",
}


def _run_dir(run_id: str) -> Path:
    return RUNS_ROOT / run_id


def _uv_env() -> dict:
    return {**os.environ, "MSWEA_COST_TRACKING": "ignore_errors"}


def _task_mounts() -> list:
    """Mounts shared by the DockerOperator tasks. Sources are HOST paths."""
    return [
        Mount(source="/var/run/docker.sock", target="/var/run/docker.sock", type="bind"),
        Mount(source=f"{HOST_PROJECT_ROOT}/runs", target=f"{CONTAINER_PROJECT}/runs", type="bind"),
        Mount(source=HOST_KEY_DIR, target="/root/.config/mini-swe-agent", type="bind", read_only=True),
    ]


def _task_env() -> dict:
    return {
        "NEBIUS_API_KEY": os.environ.get("NEBIUS_API_KEY", ""),
        "MSWEA_COST_TRACKING": "ignore_errors",
    }


# Common kwargs for every DockerOperator task.
_DOCKER_KW = dict(
    image=TASK_IMAGE,
    docker_url=DOCKER_URL,
    network_mode="bridge",
    auto_remove="success",
    mount_tmp_dir=False,
    working_dir=CONTAINER_PROJECT,
    mounts=_task_mounts(),
    environment=_task_env(),
    retries=1,
    retry_delay=timedelta(minutes=1),
    execution_timeout=timedelta(minutes=45),
)

# Default per-task resilience for the Python tasks.
_DEFAULT_ARGS = dict(
    retries=2,
    retry_delay=timedelta(seconds=30),
)


@dag(
    dag_id="evaluate_agent",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=_DEFAULT_ARGS,
    params={
        "split": Param("test", type="string", description="Dataset split, e.g. test"),
        "subset": Param("verified", type="string", enum=["verified", "lite", "full"],
                        description="SWE-bench subset"),
        "workers": Param(5, type="integer", minimum=1, description="Parallel workers"),
        "model": Param("nebius/moonshotai/Kimi-K2.6", type="string",
                       description="LiteLLM model string served via Nebius Token Factory"),
        "task_slice": Param("0:3", type=["null", "string"],
                            description="Instance slice, e.g. '0:3'. Null = whole subset."),
        "run_id": Param(None, type=["null", "string"],
                        description="Explicit run id. If null, generated from timestamp."),
        "cost_limit": Param(0, type="number", description="Per-instance cost limit."),
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
        for d in (run_dir / "run-agent", run_dir / "run-eval"):
            d.mkdir(parents=True, exist_ok=True)

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

    cfg = prepare_run()

    # Convenience: templated references pulled from prepare_run's XCom.
    RID = "{{ ti.xcom_pull(task_ids='prepare_run')['run_id'] }}"
    SUBSET = "{{ ti.xcom_pull(task_ids='prepare_run')['subset'] }}"
    SPLIT = "{{ ti.xcom_pull(task_ids='prepare_run')['split'] }}"
    MODEL = "{{ ti.xcom_pull(task_ids='prepare_run')['model'] }}"
    WORKERS = "{{ ti.xcom_pull(task_ids='prepare_run')['workers'] }}"
    SLICE = "{{ ti.xcom_pull(task_ids='prepare_run')['task_slice'] }}"
    DATASET = "{{ ti.xcom_pull(task_ids='prepare_run')['dataset_name'] }}"

    agent_out = f"{CONTAINER_PROJECT}/runs/{RID}/run-agent"
    eval_out = f"{CONTAINER_PROJECT}/runs/{RID}/run-eval"
    preds_path = f"{agent_out}/preds.json"

    # ---- run_agent: DockerOperator ----
    # Resolve the mini-swe-agent config path inside the container (version-proof),
    # then run the batch agent, writing preds.json + trajectories into the run tree.
    agent_config_expr = (
        "$(uv run python -c \"import minisweagent,os;"
        "print(os.path.join(os.path.dirname(minisweagent.__file__),"
        "'config','benchmarks','swebench.yaml'))\" | tail -1)"
    )
    run_agent = DockerOperator(
        task_id="run_agent",
        command=[
            "bash", "-lc",
            f"uv run mini-extra swebench "
            f"--subset {SUBSET} --split {SPLIT} --model {MODEL} "
            f"--config {agent_config_expr} --workers {WORKERS} -o {agent_out} "
            f"--slice {SLICE} 2>&1 | tee {agent_out}/run-agent.log",
        ],
        **_DOCKER_KW,
    )

    # ---- run_eval: DockerOperator ----
    # The harness writes its report to the working dir; cd into the run-eval dir
    # so the report lands inside the run tree.
    run_eval = DockerOperator(
        task_id="run_eval",
        command=[
            "bash", "-lc",
            f"cd {eval_out} && "
            f"uv run python -m swebench.harness.run_evaluation "
            f"--dataset_name {DATASET} --split {SPLIT} "
            f"--predictions_path {preds_path} --max_workers {WORKERS} "
            f"--run_id {RID} 2>&1 | tee {eval_out}/run-eval.log",
        ],
        **_DOCKER_KW,
    )

    @task
    def summarize_and_log(config: dict) -> dict:
        """Read preds.json + eval report from the run tree -> metrics.json,
        manifest.json, and MLflow. (Data comes from disk, not XCom.)"""
        import mlflow

        run_dir = _run_dir(config["run_id"])
        agent_out_p = run_dir / "run-agent"
        eval_out_p = run_dir / "run-eval"

        # agent-side counts from preds.json
        preds_file = agent_out_p / "preds.json"
        n_instances = n_patched = 0
        if preds_file.exists():
            preds = json.loads(preds_file.read_text())
            n_instances = len(preds)
            n_patched = sum(1 for v in preds.values() if v.get("model_patch"))

        # eval report -- the harness wrote it into run-eval/ (cwd); find it
        report = _find_report_in(eval_out_p, config["run_id"])
        report_dst = eval_out_p / "report.json"
        if report is not None and report != report_dst:
            report_dst.write_text(report.read_text())

        metrics = _parse_report(str(report_dst) if report_dst.exists() else None)
        metrics.update({"n_instances": n_instances, "n_patched": n_patched})
        metrics["total_instances"] = n_instances or metrics.get("total_instances")
        denom = n_instances or metrics.get("total_instances")
        if denom:
            metrics["resolve_rate"] = round(metrics["resolved_instances"] / denom, 4)
        (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2))

        manifest = _build_manifest(run_dir, config)
        (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))

        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment(MLFLOW_EXPERIMENT)
        with mlflow.start_run(run_name=config["run_id"]):
            mlflow.log_params({k: config[k] for k in
                ("split", "subset", "workers", "model", "task_slice", "cost_limit", "dataset_name")})
            mlflow.log_metrics({k: v for k, v in metrics.items() if isinstance(v, (int, float))})
            for name in ("config.json", "metrics.json", "manifest.json"):
                p = run_dir / name
                if p.exists():
                    mlflow.log_artifact(str(p))
            mlflow.set_tag("git_sha", manifest.get("git_sha", "unknown"))
        return metrics

    @task
    def upload_to_s3(config: dict) -> str:
        """Upload the run tree to S3-compatible storage (Phase 2 durability)."""
        run_dir = _run_dir(config["run_id"])
        env = {
            **_uv_env(),
            "AWS_ACCESS_KEY_ID": os.environ.get("S3_ACCESS_KEY", "minioadmin"),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("S3_SECRET_KEY", "minioadmin"),
        }
        subprocess.run(
            ["uv", "run", "python", "scripts/s3_upload.py",
             str(run_dir), S3_BUCKET, S3_ENDPOINT_URL, S3_REGION],
            cwd=PROJECT_ROOT, env=env, check=True,
        )
        return f"s3://{S3_BUCKET}/{config['run_id']}/"

    summary = summarize_and_log(cfg)
    up = upload_to_s3(cfg)

    # ---- wiring: prepare -> agent -> eval -> summarize -> upload ----
    cfg >> run_agent >> run_eval >> summary >> up


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _find_report_in(eval_dir: Path, run_id: str):
    """Locate the SWE-bench report json produced for this run_id (in eval dir)."""
    for pat in (f"*.{run_id}.json", f"*{run_id}*.json", "report.json"):
        found = list(eval_dir.glob(pat))
        if found:
            return found[0]
    return None


def _parse_report(report_path) -> dict:
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
