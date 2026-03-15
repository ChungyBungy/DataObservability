import json
import random
from datetime import datetime, timedelta


def generate_record(i, timestamp):

    record = {

        "record_id": i,
        "timestamp": timestamp,
        "cell_id": f"cell_{random.randint(1,5)}",
        "slice_type": random.choice(["Gold","Silver","Bronze"]),

        "ran_kpi": {

            "rrc_connection_success_rate": random.uniform(95,99.9),
            "call_setup_success_rate": random.uniform(95,99.5),

            "call_drop_rate": random.uniform(0,3),
            "rrc_drop_rate": random.uniform(0,2),

            "handover_success_rate": random.uniform(90,99),
            "handover_failure_rate": random.uniform(0,5),

            "sinr": random.uniform(5,30),
            "rsrp": random.uniform(-110,-70),
            "rsrq": random.uniform(-20,-5),

            "packet_loss_rate": random.uniform(0,2),

            "prb_utilization": random.uniform(30,95),
            "cell_throughput": random.uniform(50,500),
            "active_user_count": random.randint(10,300)
        },

        "core_kpi": {

            "session_establishment_success_rate": random.uniform(95,99.9),
            "bearer_setup_success_rate": random.uniform(95,99),

            "tracking_area_update_success_rate": random.uniform(95,99),
            "paging_success_rate": random.uniform(95,99),

            "latency": random.uniform(5,50),
            "throughput": random.uniform(100,1000),
            "jitter": random.uniform(0,10),

            "packet_loss": random.uniform(0,1),

            "node_availability": random.uniform(99,100),
            "service_failure_rate": random.uniform(0,2)
        }
    }


    
    # Missing top-level field
    if random.random() < 0.03:
        record.pop("slice_type", None)

    # Missing entire RAN KPI block
    if random.random() < 0.02:
        record.pop("ran_kpi", None)

    # Missing entire CORE KPI block
    if random.random() < 0.02:
        record.pop("core_kpi", None)

    # Missing individual RAN KPI
    if "ran_kpi" in record and random.random() < 0.05:
        k = random.choice(list(record["ran_kpi"].keys()))
        record["ran_kpi"].pop(k, None)

    # Missing individual CORE KPI
    if "core_kpi" in record and random.random() < 0.05:
        k = random.choice(list(record["core_kpi"].keys()))
        record["core_kpi"].pop(k, None)


    return record

data = []
start_time = datetime.now()

for i in range(200):

    timestamp = (start_time + timedelta(minutes=i)).isoformat()

    record = generate_record(i, timestamp)

    # Inject anomalies
    if random.random() < 0.05:
        record["ran_kpi"]["packet_loss_rate"] = None

    if random.random() < 0.05:
        del record["core_kpi"]["latency"]

    if random.random() < 0.05:
        record["ran_kpi"]["sinr"] = "BAD_SIGNAL"

    if random.random() < 0.05:
        record["timestamp"] = (start_time - timedelta(hours=2)).isoformat()

    data.append(record)


with open("telecom_data.json","w") as f:
    json.dump(data, f, indent=4)

print("Dataset generated: telecom_data.json")
print("Total records:", len(data))