import json
import os
import time
from datetime import datetime, timezone

import streamlit as st
from databricks.sdk import WorkspaceClient

ENVIRONMENTS = {
    "DEV": {
        "catalog": "retail_dev",
        "e2e_job": os.getenv("DEV_E2E_JOB_ID", ""),
        "qa_job": os.getenv("DEV_QA_JOB_ID", ""),
    },
    "STAGING": {
        "catalog": "retail_staging",
        "e2e_job": os.getenv("STAGING_E2E_JOB_ID", ""),
        "qa_job": os.getenv("STAGING_QA_JOB_ID", ""),
    },
    "PROD": {
        "catalog": "retail_prod",
        "e2e_job": os.getenv("PROD_E2E_JOB_ID", ""),
        "qa_job": os.getenv("PROD_QA_JOB_ID", ""),
    },
}

TERMINAL = {"SUCCESS", "FAILED", "TIMEDOUT", "CANCELED", "SKIPPED",
            "INTERNAL_ERROR", "MAXIMUM_CONCURRENT_RUNS_REACHED",
            "UPSTREAM_CANCELED", "UPSTREAM_FAILED", "EXCLUDED", "DISABLED"}

st.set_page_config(page_title="Retail Lakehouse QA", page_icon="🧪", layout="wide")


@st.cache_resource
def get_workspace_client():
    return WorkspaceClient()


def run_job(job_id: str, parameters: dict | None = None) -> int:
    if not job_id:
        raise RuntimeError("Job resource is not configured for this environment.")
    response = get_workspace_client().jobs.run_now(
        job_id=int(job_id),
        job_parameters=parameters or {},
        idempotency_token=f"qa-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"[:64],
    )
    return int(response.run_id)


def wait_for_run(run_id: int, label: str, timeout_seconds: int = 1800) -> dict:
    client = get_workspace_client()
    started = time.time()
    placeholder = st.empty()
    while time.time() - started < timeout_seconds:
        run = client.jobs.get_run(run_id=run_id)
        state = run.state
        life_cycle = str(getattr(state, "life_cycle_state", "") or "")
        result = str(getattr(state, "result_state", "") or "")
        placeholder.info(f"{label}: {life_cycle}" + (f" / {result}" if result else ""))
        if result in TERMINAL or life_cycle in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
            placeholder.empty()
            return {
                "run_id": run_id,
                "life_cycle_state": life_cycle,
                "result_state": result,
                "state_message": getattr(state, "state_message", ""),
                "run_page_url": getattr(run, "run_page_url", ""),
            }
        time.sleep(5)
    placeholder.error(f"{label} timed out after {timeout_seconds // 60} minutes.")
    return {"run_id": run_id, "life_cycle_state": "TIMEOUT", "result_state": "TIMEDOUT"}


def get_output(run_id: int) -> dict:
    output = get_workspace_client().jobs.get_run_output(run_id=run_id)
    raw = getattr(getattr(output, "notebook_output", None), "result", None)
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"raw": raw}


def run_qa(env: str, qa_job_id: str) -> dict:
    run_id = run_job(
        qa_job_id,
        {"environment": env.lower()},
    )
    result = wait_for_run(run_id, f"{env} QA suite")
    result["qa_run_id"] = run_id
    if result.get("result_state") == "SUCCESS":
        result["qa_results"] = get_output(run_id)
    return result


st.title("🧪 GlobalMart Retail Lakehouse — QA Automation")
st.caption("End-to-end validation for Bronze → Silver → Gold, DQ, integrity and idempotency.")

environment = st.sidebar.selectbox(
    "Environment",
    list(ENVIRONMENTS.keys()),
    index=list(ENVIRONMENTS.keys()).index(
        os.getenv("DEFAULT_ENVIRONMENT", "PROD")
    ),
)
cfg = ENVIRONMENTS[environment]

st.sidebar.markdown(f"**Catalog:** `{cfg['catalog']}`")
st.sidebar.markdown(f"**E2E Job:** `{cfg['e2e_job'] or 'not configured'}`")
st.sidebar.markdown(f"**QA Job:** `{cfg['qa_job'] or 'not configured'}`")

col1, col2 = st.columns(2)
with col1:
    run_e2e = st.button("▶ Run E2E + QA", type="primary", use_container_width=True)
with col2:
    run_only_qa = st.button("🧪 Run QA Only", use_container_width=True)

if run_e2e:
    if not cfg["e2e_job"]:
        st.error(f"{environment} E2E job is not configured.")
    elif not cfg["qa_job"]:
        st.error(f"{environment} QA job is not configured.")
    else:
        with st.status(f"Running {environment} E2E pipeline...", expanded=True) as status:
            e2e_run_id = run_job(cfg["e2e_job"], {"environment": environment.lower()})
            st.write(f"E2E run ID: `{e2e_run_id}`")
            e2e_result = wait_for_run(e2e_run_id, f"{environment} E2E")
            if e2e_result.get("result_state") != "SUCCESS":
                status.update(label="E2E failed — QA was not started", state="error")
                st.error(e2e_result)
            else:
                st.write("E2E completed successfully. Starting QA suite...")
                qa_result = run_qa(environment, cfg["qa_job"])
                if qa_result.get("result_state") == "SUCCESS":
                    status.update(label="E2E + QA completed", state="complete")
                else:
                    status.update(label="E2E succeeded but QA failed", state="error")
                st.session_state["last_result"] = {
                    "e2e": e2e_result,
                    "qa": qa_result,
                }

if run_only_qa:
    if not cfg["qa_job"]:
        st.error(f"{environment} QA job is not configured.")
    else:
        st.session_state["last_result"] = {"qa": run_qa(environment, cfg["qa_job"])}

result = st.session_state.get("last_result")
if result:
    st.divider()
    st.subheader("Latest execution")
    if "e2e" in result:
        st.write("**E2E:**", result["e2e"])
    qa = result.get("qa", {})
    qa_results = qa.get("qa_results", {})
    if qa_results:
        summary = qa_results.get("summary", {})
        m1, m2, m3 = st.columns(3)
        m1.metric("Passed", summary.get("passed", 0))
        m2.metric("Failed", summary.get("failed", 0))
        m3.metric("Warnings", summary.get("warnings", 0))
        rows = qa_results.get("tests", [])
        if rows:
            st.dataframe(rows, use_container_width=True, hide_index=True)
    elif qa:
        st.write("**QA run:**", qa)

st.divider()
st.subheader("What this QA suite validates")
st.markdown(
    """
- **Bronze:** table existence, schema/technical metadata, ingestion metrics, rescued/corrupt records.
- **Silver:** table existence, required columns, non-empty datasets, DQ metrics and quarantine behavior.
- **Gold:** business-key uniqueness, SCD2 integrity, fact counts, summary reconciliation and dimension coverage.
- **Pipeline:** E2E job success before QA execution.
- **Regression:** results are persisted in `retail_<env>.monitoring.qa_test_results`.
"""
)
