# Data Directory

This directory will be populated when you run the platform:

- `sample_events.jsonl` - Generated sample IoT telemetry events
- `dead_letter_queue.jsonl` - Failed events (if any)
- `lake/` - Data lake with date-partitioned JSON files
  - Structure: `lake/YYYY/MM/DD/telemetry_*.json`

These files are generated automatically when you run:
```bash
python src/main_simple.py
```
