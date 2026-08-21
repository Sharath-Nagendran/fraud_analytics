"""
fraud_analytics_demo_pipeline.py

Production-grade fraud analytics pipeline: ingest -> validate -> engineer
features -> train/score -> evaluate quality gate -> promote -> load curated
Postgres -> refresh ClickHouse gold tables -> refresh dashboard + emit
lineage. Built from the Fraud_Analytics_Demo_Pack_v2 data and designed to
be triggered from the unified management portal via DAG run `conf` /
Airflow `params` (see the `params=` block at the bottom).

===========================================================================
SPARK INTEGRATION -- REWRITTEN AGAINST THE REAL spark_job_api / spark_client
SOURCE (previously this was written against assumptions; those are gone).
===========================================================================
Actual topology, confirmed from app/main.py, app/services/job_service.py,
app/services/spark_service.py and spark_executor_server.py:

  Airflow --HTTP--> spark-job-api (FastAPI, builds the spark-submit
                     command AND executes it via a call to sparkf-client;
                     it is not a passive command-builder as previously
                     assumed) --HTTP--> spark-client (runs the command
                     with subprocess.run, shell=True, blocking up to a
                     hardcoded 600s)

Two real endpoints matter to us:

  POST /jobs/submit  {name, job_type, artifact_path, entry_point, args}
      -> {"job_id": "<uuid>", "status": "SUBMITTED"}
      job_type is validated against {"jar","scala","pyspark"} in the
      service code, but per your instruction ONLY "jar" is currently an
      approved/supported path operationally, so this DAG hardcodes
      job_type="jar" and always requires entry_point (a fully-qualified
      Java/Scala class name). artifact_path must be a plain HTTP(S) URL
      -- spark-job-api downloads it itself (unauthenticated `requests.get`)
      and re-uploads it to HDFS before submitting.

      Deploy mode is "cluster" (see spark_service.build_spark_command),
      and `spark.kubernetes.submission.waitAppCompletion` is left at its
      Spark default of `true`, so the initial /jobs/submit HTTP call
      itself blocks for the lifetime of the Spark application, up to the
      600s timeout hardcoded in both k8s.py's `exec_spark_submit` and
      spark_executor_server.py's `/submit` handler. Concretely: any
      training job that takes longer than ~10 minutes will time out at
      the HTTP layer even though the underlying Spark app may still be
      running on the cluster. There is no way around this from the
      Airflow side except keeping training jobs under ~9 minutes.

  GET /jobs/{job_id}  -> {"job_id", "status", "created_at"}
      `status` is refreshed by a background thread (job_service's
      startup `monitor()`) every ~10s while it's SUBMITTED/RUNNING, by
      checking the k8s driver pod's phase via label selector
      spark-app-name=<name>,spark-role=driver.

KNOWN PLATFORM ISSUES (found while reading the source, not fixed here --
these live in spark-job-api / spark-client, out of scope for a DAG
change, but they materially affect how much this DAG can trust a
"SUCCESS" from spark-job-api, so they're handled defensively below):

  1. `submit_job()` in job_service.py writes the Job row with
     status="SUBMITTED" unconditionally, WITHOUT checking the
     returncode of the spark-submit subprocess it already ran and
     already has the stdout/stderr for. A spark-submit that fails
     immediately (bad classpath, artifact 404, etc.) is silently
     recorded as "SUBMITTED" -- failure is only detected later, if at
     all, by the pod-phase reconciliation described in #2.
  2. `reconcile_job_status()`'s NOT_FOUND branch: if the driver pod
     cannot be found while a job is SUBMITTED/RUNNING, it assumes
     SUCCESS ("Pod gone for job {id}, marking SUCCESS (likely
     completed)"). A job whose driver pod never scheduled at all (e.g.
     the artifact never resolved) will eventually read back as SUCCESS,
     not FAILED. This DAG treats a SUCCESS with a *missing or stale*
     METRICS_PATH file as a hard failure rather than trusting the status
     string alone (see `_wait_for_jar_job` below).
  3. `/jobs/sql` (SqlJobRequest -> submit_sql_job) actually executes
     SYNCHRONOUSLY before the HTTP response is returned (it calls
     spark-client's /sql endpoint and commits SUCCESS/FAILED to the DB
     before `submit_sql_job()` returns) -- but the FastAPI route handler
     then discards that and always returns {"status": "SUBMITTED"}
     regardless of the real outcome. This DAG never trusts the response
     body of POST /jobs/sql; it always makes one immediate follow-up GET
     /jobs/{job_id} to read the true, already-final status.
  4. Terminal-state strings are inconsistent between code paths: the
     background reconciler writes "SUCCESS", but the (separate,
     rarely-hit) `/jobs/{job_id}/status` handler writes "COMPLETED" for
     the same underlying pod phase. This DAG's status matcher accepts
     both.
  5. No endpoint exposes `Job.logs` (stdout/stderr) over the API -- only
     the HTML `/ui/jobs/{id}` page can see it. On failure this DAG can
     only report the job_id and status, not the Spark error text; if
     you're debugging a failed run you'll need to check the job-api DB
     `jobs.logs` column or the `/ui/jobs/{id}` page directly.

Until a real training JAR is built and hosted at an HTTP(S) URL,
`fraud__use_spark_job_api` defaults to "false" and training runs
in-process with scikit-learn on the Airflow worker -- this keeps the DAG
runnable today without depending on anything not yet built.
===========================================================================
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

try:
    from airflow.models import Variable
except Exception:  # pragma: no cover
    Variable = None

try:
    from airflow.hooks.base import BaseHook
except Exception:  # pragma: no cover
    BaseHook = None

try:
    from airflow.models.param import Param
except Exception:  # pragma: no cover
    Param = None

logger = logging.getLogger(__name__)

# ============================================================
# Configuration
# ============================================================
USE_CASE = "fraud_analytics"


def _var(key: str, default=None):
    """Airflow Variable, falling back to env var, falling back to default."""
    if Variable is not None:
        try:
            return Variable.get(key, default_var=os.getenv(key, default))
        except Exception:
            pass
    return os.getenv(key, default)


def _conn_or_env(conn_id: str, host_env: str, port_env: str, user_env: str,
                  password_env: str, db_env: str, default_port: int, default_db: str):
    """
    Resolve connection details from an Airflow Connection first (the
    production-grade path -- credentials live in the secrets backend,
    not in this file or in plaintext Variables), falling back to env
    vars for local/demo runs. Never falls back to a hardcoded password;
    a missing password is a hard configuration error, not a silent
    default, because that's exactly the kind of thing that becomes a
    security incident in production.
    """
    if BaseHook is not None:
        try:
            conn = BaseHook.get_connection(conn_id)
            return {
                "host": conn.host,
                "port": conn.port or default_port,
                "user": conn.login,
                "password": conn.password,
                "db": conn.schema or default_db,
            }
        except Exception:
            pass

    host = os.getenv(host_env)
    password = os.getenv(password_env)
    if not host or not password:
        raise AirflowException(
            f"No Airflow Connection '{conn_id}' registered and env vars "
            f"{host_env}/{password_env} are not both set. Refusing to fall "
            f"back to a hardcoded default credential."
        )
    return {
        "host": host,
        "port": int(os.getenv(port_env, default_port)),
        "user": os.getenv(user_env, "postgres"),
        "password": password,
        "db": os.getenv(db_env, default_db),
    }


DEMO_DATA_DIR = _var("fraud__demo_data_dir",
                      "/opt/airflow/dags/data-platform/airflow_usecase/fraud-risk/demo_pack")
STAGING_DIR = _var("fraud__staging_dir", "/opt/airflow/staging/fraud_analytics")
MODEL_DIR = _var("fraud__model_dir", "/models/fraud_analytics")
MODEL_CANDIDATE_PATH = os.path.join(MODEL_DIR, "fraud_model_candidate.joblib")
MODEL_PRODUCTION_PATH = os.path.join(MODEL_DIR, "fraud_model_production.joblib")
METRICS_PATH = os.path.join(MODEL_DIR, "metrics_candidate.json")

CSV_FILES = ["customers.csv", "accounts.csv", "devices.csv", "merchants.csv",
             "transactions.csv", "fraud_events.csv", "alerts.csv", "cases.csv"]

CLICKHOUSE_DB = _var("fraud__clickhouse_db", "fraud_demo")

# ---- spark-job-api (see module docstring for the real contract) ----
SPARK_JOB_API_URL = _var("SPARK_JOB_API_URL", "http://jobapi.data-platform.tcs.private.cloud")
USE_SPARK_JOB_API = str(_var("fraud__use_spark_job_api", "false")).lower() == "true"
SPARK_TRAINING_ARTIFACT_PATH = _var("fraud__training_artifact_path", "")
SPARK_TRAINING_ENTRY_POINT = _var("fraud__training_entry_point", "com.fraud.TrainFraudModelJob")
# job-api's own hard timeout on the underlying spark-submit subprocess is
# 600s (spark_executor_server.py); give ourselves a small buffer above it
# so our HTTP client doesn't cut the connection first.
SPARK_SUBMIT_HTTP_TIMEOUT = int(_var("fraud__spark_submit_http_timeout_sec", "650"))
SPARK_SQL_HTTP_TIMEOUT = int(_var("fraud__spark_sql_http_timeout_sec", "320"))
SPARK_JOB_POLL_INTERVAL = int(_var("fraud__spark_job_poll_interval_sec", "10"))
SPARK_JOB_POLL_TIMEOUT = int(_var("fraud__spark_job_poll_timeout_sec", "1800"))

DATAHUB_TOKEN = _var("DATAHUB_GMS_TOKEN", "eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6InNlcnZpY2VfMDNkMTZjMTctZjFjNC00MGY0LThkNDAtNWYyM2JjZGUzZGYyIiwidHlwZSI6IlNFUlZJQ0VfQUNDT1VOVCIsInZlcnNpb24iOiIyIiwianRpIjoiNTIxMTExNzQtMTgzYS00MmQ4LWI4YTEtMjdmY2ZiMDk0OGE5Iiwic3ViIjoic2VydmljZV8wM2QxNmMxNy1mMWM0LTQwZjQtOGQ0MC01ZjIzYmNkZTNkZjIiLCJpc3MiOiJkYXRhaHViLW1ldGFkYXRhLXNlcnZpY2UifQ.cb15MFr88gDERo_7d6jceEhaccTZXZKEdoa8IGC4cwQ")
DATAHUB_GMS_URL = _var("DATAHUB_GMS_URL", "http://datahub-datahub-gms.datahub-tenant.svc.cluster.local:8080")
SUPERSET_DASHBOARD_URL = _var(
    "fraud__superset_dashboard_url",
    "http://superset.superset-tenant-a.svc.cluster.local:8088/superset/dashboard/fraud_demo/",
)

SEED = 42
DEFAULT_MIN_RECALL = 0.60
DEFAULT_MIN_PRECISION = 0.30

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 8, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


# ============================================================
# spark-job-api client (matches the REAL contract -- see docstring)
# ============================================================
_SUCCESS_STATES = {"SUCCESS", "SUCCEEDED", "COMPLETED"}  # bug #4: inconsistent strings, accept both
_FAILURE_STATES = {"FAILED", "ERROR", "CANCELLED"}
_NON_TERMINAL_STATES = {"SUBMITTED", "RUNNING", "PENDING"}


def _submit_jar_job(name: str, artifact_path: str, entry_point: str, job_args=None) -> str:
    """
    POST /jobs/submit. job_type is hardcoded to "jar" -- the schema also
    accepts "pyspark"/"scala" but only "jar" is an approved path today.
    artifact_path MUST be a plain, unauthenticated HTTP(S) URL; job-api
    downloads it itself before re-uploading to HDFS.

    NOTE: because deploy-mode is "cluster" with waitAppCompletion=true
    (see docstring), this call blocks for the job's full runtime, up to
    ~600s server-side. Expect this task to look "stuck" on this line for
    minutes at a time -- that's expected, not a hang.
    """
    if not artifact_path:
        raise AirflowException("artifact_path is required for a jar job")
    payload = {
        "name": name,
        "job_type": "jar",
        "artifact_path": artifact_path,
        "entry_point": entry_point,
        "args": job_args or [],
    }
    resp = requests.post(f"{SPARK_JOB_API_URL}/jobs/submit", json=payload, timeout=SPARK_SUBMIT_HTTP_TIMEOUT)
    resp.raise_for_status()
    body = resp.json()
    job_id = body.get("job_id")
    if not job_id:
        raise AirflowException(f"spark-job-api /jobs/submit response had no job_id: {body}")
    logger.info("Submitted jar job '%s' -> job_id=%s (this call blocks until the app finishes or ~10min elapse)",
                name, job_id)
    return job_id


def _submit_sql_job(name: str, sql: str, spark_conf: dict = None) -> str:
    """
    POST /jobs/sql. This executes SYNCHRONOUSLY server-side before the
    HTTP response comes back (see bug #3) -- but the response body always
    claims status="SUBMITTED" regardless of outcome, so we never read it.
    We immediately follow up with a single GET /jobs/{job_id} to get the
    real, already-final status.
    """
    payload = {"name": name, "sql": sql, "spark_conf": spark_conf or {}}
    resp = requests.post(f"{SPARK_JOB_API_URL}/jobs/sql", json=payload, timeout=SPARK_SQL_HTTP_TIMEOUT)
    resp.raise_for_status()
    job_id = resp.json().get("job_id")
    if not job_id:
        raise AirflowException(f"spark-job-api /jobs/sql response had no job_id: {resp.json()}")
    return job_id


def _get_job_status(job_id: str) -> str:
    resp = requests.get(f"{SPARK_JOB_API_URL}/jobs/{job_id}", timeout=30)
    resp.raise_for_status()
    return str(resp.json().get("status", "UNKNOWN")).upper()


def _wait_for_jar_job(job_id: str, expect_file: str = None, submitted_at: float = None):
    """
    Poll GET /jobs/{job_id} until a terminal state. Because the
    background reconciler will mark a job SUCCESS even if its driver pod
    never existed (platform bug #2), a SUCCESS is only trusted here if
    `expect_file` exists and was modified after `submitted_at` --
    otherwise we raise, since a stale/missing artifact means nothing
    actually ran no matter what the status string says.
    """
    elapsed = 0
    while elapsed < SPARK_JOB_POLL_TIMEOUT:
        status = _get_job_status(job_id)
        if status in _SUCCESS_STATES:
            if expect_file and (not os.path.exists(expect_file) or
                                 (submitted_at and os.path.getmtime(expect_file) < submitted_at)):
                raise AirflowException(
                    f"spark job {job_id} reported {status} but {expect_file} is missing or "
                    f"predates submission -- treating as failed (see platform bug #2 in the "
                    f"module docstring: a driver pod that never scheduled is misreported as SUCCESS)."
                )
            logger.info("spark job %s succeeded (status=%s)", job_id, status)
            return
        if status in _FAILURE_STATES:
            raise AirflowException(
                f"spark job {job_id} failed (status={status}). The job-api has no endpoint "
                f"exposing stdout/stderr (platform bug #5) -- check /ui/jobs/{job_id} or the "
                f"job-api DB `jobs.logs` column for details."
            )
        if status not in _NON_TERMINAL_STATES:
            logger.warning("spark job %s returned an unrecognized status '%s'; continuing to poll", job_id, status)
        time.sleep(SPARK_JOB_POLL_INTERVAL)
        elapsed += SPARK_JOB_POLL_INTERVAL
    raise AirflowException(f"spark job {job_id} did not reach a terminal state within {SPARK_JOB_POLL_TIMEOUT}s")


def _run_sql_job_and_wait(name: str, sql: str, spark_conf: dict = None):
    job_id = _submit_sql_job(name, sql, spark_conf)
    status = _get_job_status(job_id)  # already final by the time submit returns -- see bug #3
    if status in _FAILURE_STATES:
        raise AirflowException(f"Spark SQL job {job_id} failed (status={status}). Check /ui/jobs/{job_id}.")
    if status not in _SUCCESS_STATES:
        # Give the (already-completed-server-side) job a short grace
        # window in case of a commit race, then fail loudly rather than
        # silently proceeding.
        for _ in range(3):
            time.sleep(2)
            status = _get_job_status(job_id)
            if status in _SUCCESS_STATES:
                break
        else:
            raise AirflowException(f"Spark SQL job {job_id} did not confirm success (last status={status}).")
    logger.info("Spark SQL job '%s' completed (job_id=%s)", name, job_id)


# ============================================================
# Synthetic data fallback (schema-matched to the demo pack)
# ============================================================
def _generate_synthetic_dataset():
    rng = np.random.default_rng(SEED)
    n_customers, n_merchants, n_txn = 500, 60, 4000
    cities = [
        ("Mumbai", "Maharashtra"), ("Delhi", "Delhi"), ("Bengaluru", "Karnataka"),
        ("Chennai", "Tamil Nadu"), ("Hyderabad", "Telangana"), ("Ahmedabad", "Gujarat"),
    ]
    risk_bands = ["Low", "Medium", "High"]

    cust_city = rng.integers(0, len(cities), n_customers)
    customers = pd.DataFrame({
        "customer_id": [f"CUST{i:06d}" for i in range(1, n_customers + 1)],
        "home_city": [cities[i][0] for i in cust_city],
        "home_state": [cities[i][1] for i in cust_city],
        "customer_risk_band": rng.choice(risk_bands, n_customers, p=[0.7, 0.25, 0.05]),
    })

    merch_city = rng.integers(0, len(cities), n_merchants)
    merchants = pd.DataFrame({
        "merchant_id": [f"MER{i:05d}" for i in range(1, n_merchants + 1)],
        "merchant_category": rng.choice(
            ["Grocery", "Travel", "Restaurants", "Electronics", "Healthcare", "ATM_Cash"], n_merchants),
        "city": [cities[i][0] for i in merch_city],
        "merchant_risk_band": rng.choice(risk_bands, n_merchants, p=[0.6, 0.3, 0.1]),
    })

    cust_idx = rng.integers(0, n_customers, n_txn)
    merch_idx = rng.integers(0, n_merchants, n_txn)
    is_fraud = rng.choice([0, 1], n_txn, p=[0.98, 0.02])
    base_amount = rng.gamma(2.0, 800, n_txn)
    amount = np.where(is_fraud == 1, base_amount * rng.uniform(3, 8, n_txn), base_amount)
    txn_ts = pd.Timestamp("2026-08-01") + pd.to_timedelta(rng.integers(0, 19 * 24 * 3600, n_txn), unit="s")

    transactions = pd.DataFrame({
        "transaction_id": [f"TXN{i:09d}" for i in range(1, n_txn + 1)],
        "transaction_ts": txn_ts,
        "customer_id": customers["customer_id"].values[cust_idx],
        "channel": rng.choice(["UPI", "NEFT", "POS", "ATM", "IMPS", "CARD"], n_txn),
        "amount_inr": np.round(amount, 2),
        "merchant_id": merchants["merchant_id"].values[merch_idx],
        "merchant_category": merchants["merchant_category"].values[merch_idx],
        "city": merchants["city"].values[merch_idx],
        "device_trust_status": rng.choice(["Trusted", "Known", "New"], n_txn, p=[0.5, 0.3, 0.2]),
        "txn_count_1h": np.where(is_fraud == 1, rng.integers(3, 10, n_txn), rng.integers(0, 3, n_txn)),
        "distance_from_home_km": np.where(is_fraud == 1, rng.integers(50, 3000, n_txn), rng.integers(0, 40, n_txn)),
        "is_fraud": is_fraud,
    })
    transactions["unusual_hour_flag"] = transactions["transaction_ts"].dt.hour.isin(range(0, 6)).astype(int)

    return {"customers": customers, "merchants": merchants, "transactions": transactions,
            "accounts": pd.DataFrame(), "devices": pd.DataFrame(), "fraud_events": pd.DataFrame(),
            "alerts": pd.DataFrame(), "cases": pd.DataFrame()}


# ============================================================
# Task callables
# ============================================================
def _load_demo_data(**context):
    params = context.get("params", {}) or {}
    data_dir = params.get("demo_data_dir") or DEMO_DATA_DIR
    os.makedirs(STAGING_DIR, exist_ok=True)

    frames, source = {}, "csv"
    if os.path.isdir(data_dir) and os.path.exists(os.path.join(data_dir, "transactions.csv")):
        for fname in CSV_FILES:
            path = os.path.join(data_dir, fname)
            key = fname.replace(".csv", "")
            frames[key] = pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()
        logger.info("Loaded demo pack CSVs from %s", data_dir)
    else:
        logger.warning("Demo pack not found at %s -- generating synthetic data instead.", data_dir)
        frames = _generate_synthetic_dataset()
        source = "synthetic"

    row_counts = {}
    for key, df in frames.items():
        df.to_parquet(os.path.join(STAGING_DIR, f"{key}.parquet"), index=False)
        row_counts[key] = len(df)

    logger.info("Row counts (%s): %s", source, row_counts)
    context["ti"].xcom_push(key="row_counts", value=row_counts)
    context["ti"].xcom_push(key="source", value=source)
    if source == "synthetic" and str(params.get("fail_on_synthetic_data", "false")).lower() == "true":
        raise AirflowException(
            "demo_data_dir was not found and fail_on_synthetic_data=true was set -- "
            "refusing to silently run against synthetic data."
        )


def _validate_data(**context):
    txn = pd.read_parquet(os.path.join(STAGING_DIR, "transactions.parquet"))
    customers = pd.read_parquet(os.path.join(STAGING_DIR, "customers.parquet"))

    if txn.empty:
        raise AirflowException("No transactions available after ingestion -- cannot proceed.")

    issues = []
    dup_rate = txn["transaction_id"].duplicated().mean()
    if dup_rate > 0:
        issues.append(f"{dup_rate:.2%} duplicate transaction_id")
        txn = txn.drop_duplicates(subset=["transaction_id"])

    orphan_rate = (~txn["customer_id"].isin(customers["customer_id"])).mean()
    if orphan_rate > 0.01:
        issues.append(f"{orphan_rate:.2%} transactions reference unknown customer_id")

    null_rate = txn[["amount_inr", "customer_id", "transaction_ts"]].isna().mean().max()
    if null_rate > 0.05:
        raise AirflowException(f"Null rate {null_rate:.2%} in key columns exceeds 5% threshold: {issues}")

    txn.to_parquet(os.path.join(STAGING_DIR, "transactions.parquet"), index=False)
    logger.info("Validation passed. Non-fatal issues: %s", issues or "none")
    context["ti"].xcom_push(key="validation_issues", value=issues)


def _engineer_features(**context):
    txn = pd.read_parquet(os.path.join(STAGING_DIR, "transactions.parquet"))
    txn["transaction_ts"] = pd.to_datetime(txn["transaction_ts"])

    risk_map, trust_map = {"Low": 1, "Medium": 2, "High": 3}, {"Trusted": 0, "Known": 1, "New": 2}
    txn["txn_hour"] = txn["transaction_ts"].dt.hour
    txn["is_weekend"] = txn["transaction_ts"].dt.dayofweek.isin([5, 6]).astype(int)
    txn["device_trust_numeric"] = txn["device_trust_status"].map(trust_map).fillna(1)
    txn["merchant_risk_numeric"] = (
        txn["merchant_risk_band"].map(risk_map).fillna(1) if "merchant_risk_band" in txn.columns else 1
    )
    txn["log_amount"] = np.log1p(txn["amount_inr"].clip(lower=0))

    features_path = os.path.join(STAGING_DIR, "transactions_features.parquet")
    txn.to_parquet(features_path, index=False)
    logger.info("Engineered features for %d transactions -> %s", len(txn), features_path)


def _snapshot_via_spark_sql(**context):
    """
    Optional / experimental. Demonstrates the CORRECT way to use
    /jobs/sql for a lightweight Iceberg read -- e.g. materializing
    nessie.fraud.transactions_scored_history as a versioned snapshot --
    without needing a custom JAR at all. Disabled unless
    fraud__use_spark_job_api=true, and does not feed the rest of this
    DAG yet (feature engineering still reads the CSV/synthetic staging
    data): treat this as a validated building block for wiring the real
    Iceberg source in once it exists, not a load-bearing step today.
    """
    if not USE_SPARK_JOB_API:
        logger.info("fraud__use_spark_job_api is false -- skipping Iceberg snapshot via /jobs/sql.")
        return
    table = _var("fraud__iceberg_source_table", "fraud.transactions_scored_history")
    snapshot_path = _var("fraud__iceberg_snapshot_hdfs_path",
                          "hdfs://hdfscluster/data/lake/fraud/transactions_scored_history_snapshot")
    sql = (
        f"CREATE NAMESPACE IF NOT EXISTS nessie.fraud; "
        f"DROP TABLE IF EXISTS parquet.`{snapshot_path}`; "
        f"CREATE TABLE parquet.`{snapshot_path}` USING parquet AS "
        f"SELECT * FROM nessie.{table}"
    )
    _run_sql_job_and_wait(name="fraud-iceberg-snapshot", sql=sql)


def _train_or_score_model(**context):
    os.makedirs(MODEL_DIR, exist_ok=True)

    if USE_SPARK_JOB_API:
        if not SPARK_TRAINING_ARTIFACT_PATH:
            raise AirflowException(
                "fraud__use_spark_job_api is true but fraud__training_artifact_path "
                "(an HTTP(S) URL to the pre-built training JAR) is not set."
            )
        submitted_at = time.time()
        job_id = _submit_jar_job(
            name="fraud-train-model",
            artifact_path=SPARK_TRAINING_ARTIFACT_PATH,
            entry_point=SPARK_TRAINING_ENTRY_POINT,
            job_args=[
                "--input", os.path.join(STAGING_DIR, "transactions_features.parquet"),
                "--model-out", MODEL_CANDIDATE_PATH,
                "--metrics-out", METRICS_PATH,
            ],
        )
        _wait_for_jar_job(job_id, expect_file=METRICS_PATH, submitted_at=submitted_at)
        return

    # ---- default path: in-process training, no Spark/JAR dependency ----
    df = pd.read_parquet(os.path.join(STAGING_DIR, "transactions_features.parquet"))
    feature_cols = [c for c in [
        "amount_inr", "log_amount", "txn_count_1h", "unusual_hour_flag", "distance_from_home_km",
        "txn_hour", "is_weekend", "device_trust_numeric", "merchant_risk_numeric",
    ] if c in df.columns]
    X, y = df[feature_cols].fillna(0), df["is_fraud"].astype(int)
    min_precision = float(context.get("params", {}).get("min_precision") or DEFAULT_MIN_PRECISION)

    try:
        from sklearn.linear_model import LogisticRegression
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import precision_recall_curve, precision_score, recall_score
        from sklearn.preprocessing import StandardScaler
        import joblib

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.25, random_state=SEED, stratify=y if y.sum() > 1 else None)
        scaler = StandardScaler().fit(X_train)
        model = LogisticRegression(max_iter=5000, class_weight="balanced")
        model.fit(scaler.transform(X_train), y_train)
        proba = model.predict_proba(scaler.transform(X_test))[:, 1]

        # Pick the threshold that maximizes recall subject to the
        # precision gate, rather than a flat 0.5 cutoff which tends to
        # over-flag once class_weight="balanced" rebalances a ~2% base
        # fraud rate.
        prec, rec, thresh = precision_recall_curve(y_test, proba)
        candidates = [(r, t) for p, r, t in zip(prec[:-1], rec[:-1], thresh) if p >= min_precision]
        best_threshold = max(candidates)[1] if candidates else 0.5
        preds = (proba >= best_threshold).astype(int)

        metrics = {
            "recall": float(recall_score(y_test, preds, zero_division=0)),
            "precision": float(precision_score(y_test, preds, zero_division=0)),
            "threshold": float(best_threshold),
            "n_train": int(len(X_train)), "n_test": int(len(X_test)),
            "trained_at": datetime.utcnow().isoformat(),
            "method": "sklearn_logistic_regression",
        }
        joblib.dump({"model": model, "scaler": scaler, "feature_cols": feature_cols,
                     "threshold": best_threshold}, MODEL_CANDIDATE_PATH)
    except ImportError:
        logger.warning("scikit-learn not available; using a rule-based fallback scorer.")
        threshold = X["amount_inr"].quantile(0.97)
        preds = ((X["amount_inr"] > threshold) | (X["distance_from_home_km"] > 500)).astype(int)
        tp, fp, fn = (int(((preds == 1) & (y == 1)).sum()), int(((preds == 1) & (y == 0)).sum()),
                      int(((preds == 0) & (y == 1)).sum()))
        metrics = {
            "recall": tp / (tp + fn) if (tp + fn) else 0.0,
            "precision": tp / (tp + fp) if (tp + fp) else 0.0,
            "n_train": 0, "n_test": int(len(X)),
            "trained_at": datetime.utcnow().isoformat(), "method": "rule_based_fallback",
        }
        with open(MODEL_CANDIDATE_PATH, "w") as f:
            json.dump({"rule": "amount_p97_or_distance_gt_500km", "threshold": float(threshold)}, f)

    with open(METRICS_PATH, "w") as f:
        json.dump(metrics, f)
    logger.info("Candidate model metrics: %s", metrics)


def _evaluate_model(**context):
    params = context.get("params", {}) or {}
    min_recall = float(params.get("min_recall") or DEFAULT_MIN_RECALL)
    min_precision = float(params.get("min_precision") or DEFAULT_MIN_PRECISION)

    with open(METRICS_PATH) as f:
        metrics = json.load(f)
    logger.info("Candidate metrics: %s (gate: recall>=%.2f, precision>=%.2f)", metrics, min_recall, min_precision)
    if metrics["recall"] < min_recall or metrics["precision"] < min_precision:
        raise AirflowException(
            f"Candidate model failed quality gate (recall={metrics['recall']:.2f}, "
            f"precision={metrics['precision']:.2f}). Production model is unchanged."
        )
    context["ti"].xcom_push(key="metrics", value=metrics)


def _promote_model(**context):
    import shutil
    shutil.copyfile(MODEL_CANDIDATE_PATH, MODEL_PRODUCTION_PATH)
    logger.info("Promoted %s -> %s", MODEL_CANDIDATE_PATH, MODEL_PRODUCTION_PATH)


def _load_curated_postgres(**context):
    import psycopg2
    from psycopg2.extras import execute_values

    pg = _conn_or_env("fraud_postgres_default", "MY_POSTGRES_HOST", "MY_POSTGRES_PORT",
                       "MY_POSTGRES_USER", "MY_POSTGRES_PASSWORD", "MY_POSTGRES_DB", 5432, "data_warehouse")

    txn = pd.read_parquet(os.path.join(STAGING_DIR, "transactions_features.parquet"))
    cols = [c for c in ["transaction_id", "transaction_ts", "customer_id", "channel", "amount_inr",
                         "merchant_id", "merchant_category", "city", "device_trust_status", "is_fraud"]
            if c in txn.columns]
    txn = txn[cols].where(pd.notna(txn[cols]), None)

    conn = psycopg2.connect(host=pg["host"], port=pg["port"], dbname=pg["db"],
                             user=pg["user"], password=pg["password"], connect_timeout=10)
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fraud_transactions_curated (
                transaction_id TEXT PRIMARY KEY, transaction_ts TIMESTAMP, customer_id TEXT,
                channel TEXT, amount_inr NUMERIC, merchant_id TEXT, merchant_category TEXT,
                city TEXT, device_trust_status TEXT, is_fraud INT, load_date DATE DEFAULT CURRENT_DATE
            )
        """)
        # Upsert rather than DELETE+INSERT: idempotent under reruns/backfills
        # and never leaves the table momentarily empty for a concurrent reader.
        set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != "transaction_id")
        execute_values(
            cur,
            f"INSERT INTO fraud_transactions_curated ({', '.join(cols)}) VALUES %s "
            f"ON CONFLICT (transaction_id) DO UPDATE SET {set_clause}, load_date = CURRENT_DATE",
            [tuple(row) for row in txn.itertuples(index=False)],
        )
        conn.commit()
        logger.info("Upserted %d rows into fraud_transactions_curated", len(txn))
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _refresh_clickhouse_gold(**context):
    import clickhouse_connect

    ch = None
    if BaseHook is not None:
        try:
            conn = BaseHook.get_connection("clickhouse_default")
            ch = {"host": conn.host, "port": conn.port or 8123, "user": conn.login or "default",
                  "password": conn.password or "", "db": conn.schema or CLICKHOUSE_DB}
        except Exception:
            pass
    if ch is None:
        host = os.getenv("CLICKHOUSE_HOST")
        if not host:
            raise AirflowException(
                "No Airflow Connection 'fraud_clickhouse_default' and CLICKHOUSE_HOST env var not set."
            )
        ch = {"host": host, "port": int(os.getenv("CLICKHOUSE_PORT", "8123")),
              "user": os.getenv("CLICKHOUSE_USER", "default"),
              "password": os.getenv("CLICKHOUSE_PASSWORD", ""), "db": os.getenv("CLICKHOUSE_DB", CLICKHOUSE_DB)}

    txn = pd.read_parquet(os.path.join(STAGING_DIR, "transactions_features.parquet"))
    txn["transaction_ts"] = pd.to_datetime(txn["transaction_ts"])
    txn["business_date"] = txn["transaction_ts"].dt.date
    txn["loaded_at"] = pd.Timestamp.utcnow().tz_localize(None)

    gold = (
        txn.groupby(["business_date", "channel", "city"])
        .agg(transaction_count=("transaction_id", "count"), transaction_amount_inr=("amount_inr", "sum"),
             fraud_count=("is_fraud", "sum"))
        .reset_index()
    )
    gold["fraud_amount_inr"] = (
        txn.assign(_fraud_amt=txn["amount_inr"] * txn["is_fraud"])
        .groupby(["business_date", "channel", "city"])["_fraud_amt"].sum().values
    )
    gold["fraud_rate_pct"] = (100.0 * gold["fraud_count"] / gold["transaction_count"]).round(3)
    gold["loaded_at"] = pd.Timestamp.utcnow().tz_localize(None)

    client = clickhouse_connect.get_client(host=ch["host"], port=ch["port"], username=ch["user"],
                                            password=ch["password"], database=ch["db"])
    # ReplacingMergeTree(loaded_at) + query with FINAL (or a scheduled
    # OPTIMIZE) is the idempotent pattern here: MergeTree with a blanket
    # DELETE+INSERT would either need a slow synchronous mutation or risk
    # duplicate rows on reruns, since ClickHouse has no native upsert.
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {ch['db']}.gold_daily_channel_city (
            business_date Date, channel LowCardinality(String), city LowCardinality(String),
            transaction_count UInt32, transaction_amount_inr Float64,
            fraud_count UInt32, fraud_amount_inr Float64, fraud_rate_pct Float32,
            loaded_at DateTime
        ) ENGINE=ReplacingMergeTree(loaded_at) ORDER BY (business_date, channel, city)
    """)
    client.insert_df(f"{ch['db']}.gold_daily_channel_city", gold)
    logger.info("Refreshed %d gold rows in ClickHouse (%s.gold_daily_channel_city)", len(gold), ch["db"])


def _publish_dashboard_link(**context):
    logger.info("Superset dashboard: %s", SUPERSET_DASHBOARD_URL)

def _emit_lineage(**context):
    """
    Lightweight DataHub REST emit via the legacy snapshot-ingest endpoint
    (kept as plain requests.post rather than the acryl-datahub SDK -- the
    SDK needs to be baked into the worker image, which has been unreliable
    in this environment; revisit once that's sorted out). Deliberately
    non-fatal: lineage cataloging shouldn't take down a production run.
    """
    datasets = [
        # (urn, platform, name, description)
        ("urn:li:dataset:(urn:li:dataPlatform:file,fraud_demo.transactions,PROD)",
         "file", "transactions", "Raw fraud transaction events (Kafka-sourced)."),
        ("urn:li:dataset:(urn:li:dataPlatform:postgres,fraud_demo.fraud_transactions_curated,PROD)",
         "postgres", "fraud_transactions_curated", "Curated/enriched transactions loaded by the fraud pipeline."),
        ("urn:li:dataset:(urn:li:dataPlatform:clickhouse,fraud_demo.gold_daily_channel_city,PROD)",
         "clickhouse", "gold_daily_channel_city", "Daily fraud aggregate by channel/city, serves the Superset dashboard."),
    ]
    headers = {"Authorization": f"Bearer {DATAHUB_TOKEN}"} if DATAHUB_TOKEN else {}

    ok, failed = [], []
    for urn, platform, name, description in datasets:
        aspects = [
            {"com.linkedin.common.Status": {"removed": False}},
            {"com.linkedin.dataset.DatasetProperties": {
                "name": name,
                "description": description,
                "customProperties": {"pipeline": "fraud_analytics_demo_pipeline"},
            }},
            {"com.linkedin.common.BrowsePaths": {
                "paths": [f"/prod/{platform}/fraud_demo"],
            }},
        ]
        try:
            resp = requests.post(
                f"{DATAHUB_GMS_URL}/entities?action=ingest",
                json={"entity": {"value": {"com.linkedin.metadata.snapshot.DatasetSnapshot":
                      {"urn": urn, "aspects": aspects}}}},
                headers=headers,
                timeout=15,
            )
            resp.raise_for_status()
            logger.info("DataHub ingest OK urn=%s status=%s", urn, resp.status_code)
            ok.append(urn)
        except requests.exceptions.RequestException as exc:
            body = getattr(exc.response, "text", "")[:500] if getattr(exc, "response", None) else ""
            logger.error("DataHub ingest FAILED urn=%s error=%s response_body=%s", urn, exc, body)
            failed.append(urn)

    logger.info("DataHub lineage emit: %d/%d succeeded (%s)", len(ok), len(datasets),
                "all ok" if not failed else f"failed={failed}")
  
    logger.info("DataHub lineage emit: %d/%d succeeded (%s)", len(ok), len(datasets),
                "all ok" if not failed else f"failed={failed}")

# ============================================================
# DAG
# ============================================================
_params = {}
if Param is not None:
    _params = {
        "demo_data_dir": Param(DEMO_DATA_DIR, type="string",
                                description="Path (on the Airflow worker) to the demo_pack CSVs."),
        "min_recall": Param(DEFAULT_MIN_RECALL, type="number", minimum=0, maximum=1,
                             description="Quality gate: minimum recall for promotion."),
        "min_precision": Param(DEFAULT_MIN_PRECISION, type="number", minimum=0, maximum=1,
                                description="Quality gate: minimum precision for promotion."),
        "fail_on_synthetic_data": Param(False, type="boolean",
                                         description="Fail instead of silently using synthetic data "
                                                      "if demo_data_dir isn't found."),
    }

with DAG(
    dag_id="fraud_analytics_demo_pipeline",
    default_args=default_args,
    description="Fraud analytics: ingest -> validate -> engineer -> train/score -> evaluate -> "
                 "promote -> curate -> serve -> lineage",
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    tags=["fraud-analytics", "production", "unified-portal"],
    params=_params,
    doc_md=__doc__,
) as dag:

    with TaskGroup(group_id="ingestion") as ingestion:
        load_demo_data = PythonOperator(task_id="load_transaction_data", python_callable=_load_demo_data)
        validate_data = PythonOperator(task_id="validate_transaction_data", python_callable=_validate_data)
        engineer_features = PythonOperator(task_id="engineer_risk_features", python_callable=_engineer_features)
        load_demo_data >> validate_data >> engineer_features

    with TaskGroup(group_id="modeling") as modeling:
        snapshot_via_spark_sql = PythonOperator(task_id="snapshot_iceberg_source_optional",
                                                  python_callable=_snapshot_via_spark_sql)
        train_or_score_model = PythonOperator(task_id="train_or_score_fraud_model",
                                               python_callable=_train_or_score_model)
        evaluate_model = PythonOperator(task_id="evaluate_model_quality_gate", python_callable=_evaluate_model)
        promote_model = PythonOperator(task_id="promote_model_to_production", python_callable=_promote_model)
        snapshot_via_spark_sql >> train_or_score_model >> evaluate_model >> promote_model

    with TaskGroup(group_id="serving") as serving:
        load_curated_postgres = PythonOperator(task_id="load_curated_transactions_postgres",
                                                python_callable=_load_curated_postgres)
        refresh_clickhouse_gold = PythonOperator(task_id="refresh_clickhouse_gold_tables",
                                                  python_callable=_refresh_clickhouse_gold)
        publish_dashboard_link = PythonOperator(task_id="refresh_executive_dashboard",
                                                 python_callable=_publish_dashboard_link)
        load_curated_postgres >> refresh_clickhouse_gold >> publish_dashboard_link

    emit_lineage = PythonOperator(task_id="emit_lineage_to_datahub", python_callable=_emit_lineage)

    ingestion >> modeling >> serving >> emit_lineage
