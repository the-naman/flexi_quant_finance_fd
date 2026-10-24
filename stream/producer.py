"""5.14: synthetic FQF payment events to Kafka (run by .github/workflows/stream.yml in GitHub Actions).
Writes as the Kafka user lh_producer (write-only on fqf_payment_events_*); connection from the GitHub secrets
KAFKA_BOOTSTRAP, KAFKA_CA_CERT, KAFKA_PRODUCER_PASSWORD. No personal data: customer_ref and loan_ref are synthetic.
Each run also plants the cases the pipeline must handle: a failure burst (velocity alert), duplicates, a 2-hours-late
event and one broken message. Prints counts only.  python -m stream.producer --env test [--events 300] [--dry-run]"""

import argparse
import json
import os
import random
import sys
import uuid
from datetime import UTC, datetime, timedelta
import yaml

p = argparse.ArgumentParser()
p.add_argument("--env", required=True, choices=["dev", "test", "prod"])
p.add_argument("--events", type=int)
p.add_argument("--dry-run", action="store_true")
p.add_argument("--scheduled", action="store_true")
a = p.parse_args()
with open("config/stream.yml") as f:
    scfg = yaml.safe_load(f)
if a.scheduled and not scfg["producer"]["live"].get(a.env):
    print(f"{a.env}: scheduled sends are off (config/stream.yml producer.live)")
    sys.exit(0)
n = a.events or scfg["producer"]["events"]
now = datetime.now(UTC).replace(microsecond=0)
rnd = random.Random()
METHODS, REASONS = (
    ["upi", "card", "netbanking", "nach"],
    ["insufficient_funds", "bank_down", "limit_exceeded", "otp_failed"],
)


def event(cust=None, status=None, ts=None):
    status = status or ("failed" if rnd.random() < 0.08 else "success")
    return {
        "event_id": uuid.uuid4().hex,
        "event_ts": (ts or now - timedelta(seconds=rnd.randint(0, 600))).isoformat(),
        "customer_ref": cust or f"SC{rnd.randint(1, 400):04d}",
        "loan_ref": f"SL{rnd.randint(1, 900):04d}",
        "amount_paise": rnd.randint(50, 500) * 1000,
        "status": status,
        "method": rnd.choice(METHODS),
        "reason": rnd.choice(REASONS) if status == "failed" else None,
    }


msgs = [event() for _ in range(n)]
burst = f"SC9{rnd.randint(100, 999)}"  # one customer fails 4 times in 40 seconds,
start = now.replace(minute=now.minute - now.minute % 10, second=0)  # all inside the current 10-minute window
msgs += [event(burst, "failed", start + timedelta(seconds=10 * k)) for k in range(4)]
msgs += [dict(m) for m in rnd.sample(msgs, 3)]  # the same event sent again (duplicates)
msgs.append(event(ts=now - timedelta(hours=2)))  # far too late: must be dropped
msgs.append("{broken message")  # not an event: stays in Bronze only
print(f"{a.env}: {len(msgs)} messages ({n} events, 4 burst, 3 duplicates, 1 late, 1 broken)")
if a.dry_run:
    print("(dry run: nothing sent)")
    sys.exit(0)

from confluent_kafka import Producer  # noqa: E402 (only needed when sending)

prod = Producer(
    {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": scfg["producer_user"],
        "sasl.password": os.environ["KAFKA_PRODUCER_PASSWORD"],
        "ssl.ca.pem": os.environ["KAFKA_CA_CERT"],
        "acks": "all",
        "enable.idempotence": True,
    }
)
errors = []
topic = scfg["topic"].format(env=a.env)
for m in msgs:
    key = (m.get("customer_ref") if isinstance(m, dict) else "broken").encode()
    prod.produce(
        topic,
        key=key,
        value=(json.dumps(m) if isinstance(m, dict) else m).encode(),
        on_delivery=lambda err, _msg: err and errors.append(str(err)),
    )
    prod.poll(0)
left = prod.flush(30)
if errors or left:
    sys.exit(f"{len(errors)} messages failed, {left} not delivered: {errors[:1]}")
print(f"sent {len(msgs)} messages to {topic}")
