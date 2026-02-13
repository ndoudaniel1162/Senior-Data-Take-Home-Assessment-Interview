from datetime import datetime
from typing import Dict, Any


def transform_for_operational(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize lake record into operational DB shape.
    Safe to call multiple times (deterministic).
    """

    out = dict(record)

    # normalize timestamp
    ts = out.get("timestamp")
    if isinstance(ts, str):
        out["timestamp"] = ts
    else:
        out["timestamp"] = datetime.utcfromtimestamp(ts).isoformat()

    # derive event_id if missing
    if "event_id" not in out:
        out["event_id"] = f"{out['device_id']}_{out['timestamp']}"

    # ensure schema_version
    out.setdefault("schema_version", "v1")

    return out