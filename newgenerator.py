import random
import pandas as pd
from datetime import datetime, timedelta
import math

# -------------------------------
# CONFIG
# -------------------------------
GNB_CELLS = {
    "gNB-001": ["Cell-A1", "Cell-A2", "Cell-A3"],
    "gNB-002": ["Cell-B1", "Cell-B2", "Cell-B3"]
}

CORE_NODES = {
    "AMF": ["amf-01", "amf-02"],
    "SMF": ["smf-01", "smf-02"],
    "UPF": ["upf-01", "upf-02"]
}

TIMESTEPS = [0, 15, 30]

# -------------------------------
# HELPERS
# -------------------------------
def smooth_variation(base, variation_pct=0.1):
    change = base * random.uniform(-variation_pct, variation_pct)
    return max(0, base + change)

# -------------------------------
# RAN GENERATOR (WITH ROW TARGET)
# -------------------------------
def generate_ran(target_rows):
    rows = []
    start_time = datetime(2026, 3, 24, 8, 0, 0)

    cells = [(g, c) for g in GNB_CELLS for c in GNB_CELLS[g]]
    rows_per_window = len(cells) * 3
    windows_needed = math.ceil(target_rows / rows_per_window)

    for w in range(windows_needed):
        window_start = start_time + timedelta(minutes=30 * w)

        for gnb, cell in cells:
            base_dl_prb = random.randint(100, 250)
            base_dl_tp = random.uniform(400, 900)

            for t in TIMESTEPS:
                timestamp = window_start + timedelta(minutes=t)

                dl_prb_used = int(smooth_variation(base_dl_prb))
                ul_prb_used = int(dl_prb_used * random.uniform(0.6, 0.9))
                load = dl_prb_used / 273

                rrc_attempts = int(smooth_variation(random.randint(800, 1800)))
                rrc_successes = int(rrc_attempts * random.uniform(0.92, 0.99))

                ho_attempts = random.randint(30, 250)
                ho_successes = int(ho_attempts * random.uniform(0.85, 0.97))

                dl_tp = smooth_variation(base_dl_tp)
                ul_tp = dl_tp * random.uniform(0.2, 0.4)

                if load > 0.9:
                    delay = random.uniform(8, 12)
                    loss = random.uniform(0.05, 0.1)
                elif load > 0.7:
                    delay = random.uniform(4, 8)
                    loss = random.uniform(0.02, 0.05)
                else:
                    delay = random.uniform(2, 4)
                    loss = random.uniform(0.01, 0.02)

                rows.append({
                    "timestamp": timestamp.isoformat() + "Z",
                    "gnb_id": gnb,
                    "cell_id": cell,
                    "dl_prb_used": dl_prb_used,
                    "dl_prb_total": 273,
                    "ul_prb_used": ul_prb_used,
                    "ul_prb_total": 273,
                    "rrc_setup_attempts": rrc_attempts,
                    "rrc_setup_successes": rrc_successes,
                    "handover_attempts": ho_attempts,
                    "handover_successes": ho_successes,
                    "dl_throughput_mbps": round(dl_tp, 2),
                    "ul_throughput_mbps": round(ul_tp, 2),
                    "packet_loss_percent": round(loss, 3),
                    "pdcp_delay_ms": round(delay, 2)
                })

                if len(rows) >= target_rows:
                    return pd.DataFrame(rows)

    return pd.DataFrame(rows)

# -------------------------------
# CORE GENERATOR (WITH ROW TARGET)
# -------------------------------
def generate_core(target_rows):
    rows = []
    start_time = datetime(2026, 3, 24, 8, 0, 0)

    nfs = [(t, i) for t in CORE_NODES for i in CORE_NODES[t]]
    rows_per_window = len(nfs) * 3
    windows_needed = math.ceil(target_rows / rows_per_window)

    for w in range(windows_needed):
        window_start = start_time + timedelta(minutes=30 * w)

        for nf_type, nf_id in nfs:
            base_cpu = random.uniform(40, 80)
            base_mem = random.uniform(40, 75)

            for t in TIMESTEPS:
                timestamp = window_start + timedelta(minutes=t)

                cpu = min(95, smooth_variation(base_cpu))
                mem = min(90, smooth_variation(base_mem))

                row = {
                    "timestamp": timestamp.isoformat() + "Z",
                    "nf_type": nf_type,
                    "nf_id": nf_id,
                    "registration_attempts": None,
                    "registration_successes": None,
                    "pdu_session_attempts": None,
                    "pdu_session_successes": None,
                    "session_releases": None,
                    "n3_dl_packets": None,
                    "n3_ul_packets": None,
                    "n3_dl_drop": None,
                    "n3_ul_drop": None,
                    "cpu_percent": round(cpu, 2),
                    "memory_percent": round(mem, 2)
                }

                if nf_type == "AMF":
                    attempts = int(smooth_variation(random.randint(4000, 6000)))
                    successes = int(attempts * random.uniform(0.97, 0.995))
                    row.update({
                        "registration_attempts": attempts,
                        "registration_successes": successes
                    })

                elif nf_type == "SMF":
                    attempts = int(smooth_variation(random.randint(5000, 8000)))
                    successes = int(attempts * random.uniform(0.96, 0.995))
                    releases = random.randint(100, 400)
                    row.update({
                        "pdu_session_attempts": attempts,
                        "pdu_session_successes": successes,
                        "session_releases": releases
                    })

                elif nf_type == "UPF":
                    dl_packets = int(smooth_variation(random.randint(10_000_000, 15_000_000)))
                    ul_packets = int(dl_packets * random.uniform(0.6, 0.8))

                    if cpu > 80:
                        dl_drop = random.randint(1000, 5000)
                        ul_drop = random.randint(500, 3000)
                    else:
                        dl_drop = random.randint(100, 1000)
                        ul_drop = random.randint(50, 500)

                    row.update({
                        "n3_dl_packets": dl_packets,
                        "n3_ul_packets": ul_packets,
                        "n3_dl_drop": dl_drop,
                        "n3_ul_drop": ul_drop
                    })

                rows.append(row)

                if len(rows) >= target_rows:
                    return pd.DataFrame(rows)

    return pd.DataFrame(rows)

# -------------------------------
# MAIN
# -------------------------------
if __name__ == "__main__":
    ran_rows = 1000
    core_rows = 1000

    ran_df = generate_ran(ran_rows)
    core_df = generate_core(core_rows)

    ran_df.to_csv("ran_data.csv", index=False)
    core_df.to_csv("core_data.csv", index=False)

    print(f"✅ Generated {len(ran_df)} RAN rows and {len(core_df)} CORE rows")