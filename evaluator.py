import json
from datetime import datetime

CHECK_STALE_DATA = False

SCHEMA = {
    "record_id": int,
    "timestamp": str,
    "cell_id": str,
    "slice_type": str,
    "ran_kpi": dict,
    "core_kpi": dict
}


RAN_KPIS = [
    "rrc_connection_success_rate",
    "call_setup_success_rate",
    "call_drop_rate",
    "rrc_drop_rate",
    "handover_success_rate",
    "handover_failure_rate",
    "sinr",
    "rsrp",
    "rsrq",
    "packet_loss_rate",
    "prb_utilization",
    "cell_throughput",
    "active_user_count"
]


CORE_KPIS = [
    "session_establishment_success_rate",
    "bearer_setup_success_rate",
    "tracking_area_update_success_rate",
    "paging_success_rate",
    "latency",
    "throughput",
    "jitter",
    "packet_loss",
    "node_availability",
    "service_failure_rate"
]


def validate_schema(record):

    issues = []

    for field, dtype in SCHEMA.items():

        if field not in record:
            issues.append((field,"MISSING_FIELD"))

        elif not isinstance(record[field], dtype):
            issues.append((field,"TYPE_MISMATCH"))

    return issues


def detect_nulls(record):

    issues = []

    def scan(obj, path=""):

        if isinstance(obj, dict):

            for k,v in obj.items():
                scan(v, f"{path}.{k}" if path else k)

        else:

            if obj is None:
                issues.append((path,"NULL_VALUE"))

    scan(record)

    return issues



def check_kpis(record):

    issues = []

    # ---- RAN KPI block check ----
    if "ran_kpi" not in record:
        issues.append(("ran_kpi", "MISSING_RAN_BLOCK"))

    else:
        for kpi in RAN_KPIS:
            if kpi not in record["ran_kpi"]:
                issues.append((kpi, "MISSING_RAN_KPI"))

    # ---- CORE KPI block check ----
    if "core_kpi" not in record:
        issues.append(("core_kpi", "MISSING_CORE_BLOCK"))

    else:
        for kpi in CORE_KPIS:
            if kpi not in record["core_kpi"]:
                issues.append((kpi, "MISSING_CORE_KPI"))

    return issues


def freshness_check(timestamp):

    if not CHECK_STALE_DATA:
        return None

    ts = datetime.fromisoformat(timestamp)
    delay = (datetime.now() - ts).total_seconds()

    if delay > 300:
        return "STALE_DATA"

    return None


# Load dataset
with open("telecom_data.json") as f:
    data = json.load(f)


issues_log = []


for record in data:

    schema_issues = validate_schema(record)
    null_issues = detect_nulls(record)
    kpi_issues = check_kpis(record)

    freshness_issue = freshness_check(record["timestamp"])

    for issue in schema_issues + null_issues + kpi_issues:

        issues_log.append({
            "record_id": record["record_id"],
            "issue": issue[1],
            "field": issue[0]
        })

    if freshness_issue:

        issues_log.append({
            "record_id": record["record_id"],
            "issue": freshness_issue
        })


with open("data_quality_log.json","w") as f:
    json.dump(issues_log, f, indent=4)


affected_records = len(set(issue["record_id"] for issue in issues_log if "record_id" in issue))


print("Records processed:", len(data))
print("Records with issues:", affected_records)
print("Total issues detected:", len(issues_log))
print("Log file generated: data_quality_log.json")