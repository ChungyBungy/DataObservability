import pandas as pd
import json

# -------------------------------
# EXPECTED SCHEMAS
# -------------------------------
RAN_SCHEMA = [
    "timestamp", "gnb_id", "cell_id",
    "dl_prb_used", "dl_prb_total",
    "ul_prb_used", "ul_prb_total",
    "rrc_setup_attempts", "rrc_setup_successes",
    "handover_attempts", "handover_successes",
    "dl_throughput_mbps", "ul_throughput_mbps",
    "packet_loss_percent", "pdcp_delay_ms"
]

CORE_SCHEMA = [
    "timestamp", "nf_type", "nf_id",
    "registration_attempts", "registration_successes",
    "pdu_session_attempts", "pdu_session_successes",
    "session_releases",
    "n3_dl_packets", "n3_ul_packets",
    "n3_dl_drop", "n3_ul_drop",
    "cpu_percent", "memory_percent"
]

# -------------------------------
# CORE VALIDATION LOGIC
# -------------------------------
def validate_core(df):
    errors = []

    # -------------------------------
    # 1. GLOBAL SCHEMA CHECK
    # -------------------------------
    for col in CORE_SCHEMA:
        if col not in df.columns:
            errors.append({
                "dataset": "CORE",
                "error_type": "MISSING_COLUMN",
                "column": col
            })

    # Detect EXTRA columns
    for col in df.columns:
        if col not in CORE_SCHEMA:
            errors.append({
                "dataset": "CORE",
                "error_type": "UNEXPECTED_COLUMN",
                "column": col
            })

    # -------------------------------
    # 2. DOMAIN RULES
    # -------------------------------
    CORE_RULES = {
        "AMF": {
            "required": ["registration_attempts", "registration_successes"],
            "forbidden": ["pdu_session_attempts", "pdu_session_successes", "session_releases", "n3_dl_packets"]
        },
        "SMF": {
            "required": ["pdu_session_attempts", "pdu_session_successes", "session_releases"],
            "forbidden": ["registration_attempts", "registration_successes", "n3_dl_packets"]
        },
        "UPF": {
            "required": ["n3_dl_packets", "n3_ul_packets", "n3_dl_drop"],
            "forbidden": ["registration_attempts", "registration_successes", "pdu_session_attempts"]
        }
    }

    # -------------------------------
    # 3. ROW VALIDATION
    # -------------------------------
    for _, row in df.iterrows():
        key = f"{row.get('timestamp')} | {row.get('nf_id')}"
        nf_type = row.get("nf_type")

        if nf_type not in CORE_RULES:
            continue

        rules = CORE_RULES[nf_type]

        # ✅ REQUIRED fields missing (NULL)
        for col in rules["required"]:
            if pd.isna(row.get(col)):
                errors.append({
                    "dataset": "CORE",
                    "key": key,
                    "error_type": "MISSING_REQUIRED_VALUE",
                    "column": col
                })

        # ❌ FORBIDDEN fields populated
        for col in rules["forbidden"]:
            if col in df.columns and not pd.isna(row.get(col)):
                errors.append({
                    "dataset": "CORE",
                    "key": key,
                    "error_type": "FORBIDDEN_FIELD_PRESENT",
                    "column": col
                })

        # Generic null checks (cpu/memory)
        for col in ["cpu_percent", "memory_percent"]:
            if pd.isna(row.get(col)):
                errors.append({
                    "dataset": "CORE",
                    "key": key,
                    "error_type": "NULL_VALUE",
                    "column": col
                })

    return errors

# -------------------------------
# RAN VALIDATION LOGIC
# -------------------------------
def validate_ran(df):
    errors = []

    # -------------------------------
    # 1. GLOBAL SCHEMA CHECK
    # -------------------------------
    for col in RAN_SCHEMA:
        if col not in df.columns:
            errors.append({
                "dataset": "RAN",
                "error_type": "MISSING_COLUMN",
                "column": col
            })

    # Detect EXTRA columns
    for col in df.columns:
        if col not in RAN_SCHEMA:
            errors.append({
                "dataset": "RAN",
                "error_type": "UNEXPECTED_COLUMN",
                "column": col
            })

    # -------------------------------
    # 2. ROW VALIDATION
    # -------------------------------
    for _, row in df.iterrows():
        key = f"{row.get('timestamp')} | {row.get('gnb_id')}"

        for col in RAN_SCHEMA:
            if col in ["timestamp", "gnb_id", "cell_id"]:
                continue

            # Missing OR null
            if pd.isna(row.get(col)):
                errors.append({
                    "dataset": "RAN",
                    "key": key,
                    "error_type": "MISSING_VALUE",
                    "column": col
                })

    return errors

# -------------------------------
# MAIN EVALUATOR
# -------------------------------
def evaluate(ran_csv, core_csv, output_file="error_log.json"):
    ran_df = pd.read_csv(ran_csv)
    core_df = pd.read_csv(core_csv)

    errors = []

    errors.extend(validate_ran(ran_df))
    errors.extend(validate_core(core_df))

    result = {"errors": errors}

    with open(output_file, "w") as f:
        json.dump(result, f, indent=4)

    print(f"✅ Validation complete. Found {len(errors)} issues.")
    print(f"📄 Log saved to {output_file}")

# -------------------------------
# RUN
# -------------------------------
if __name__ == "__main__":
    evaluate("ran_data.csv", "core_data.csv")   