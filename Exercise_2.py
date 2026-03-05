"""
Task 2: Log File Stream Processing with Generators
===================================================

You are building a lightweight log-monitoring tool.
Logs arrive as a large stream — you must NOT load everything into memory at once.
Use generators (yield) to build a processing pipeline.

Requirements:
    1. `read_logs(lines)` — generator that yields one log line at a time from the
       provided iterable (simulates reading a huge file lazily).
    2. `parse_log(lines)` — generator that takes raw log strings and yields
       parsed dicts: {"timestamp": str, "level": str, "message": str}.
       Log format: "YYYY-MM-DD HH:MM:SS | LEVEL | message text"
    3. `filter_by_level(logs, level)` — generator that yields only logs
       matching the given level (e.g. "ERROR").
    4. `batch(items, size)` — generator that groups items into batches
       (lists) of the given size. The last batch may be smaller.

Compose these generators into a pipeline and print the results.

Expected output:
    - Parsed ERROR logs in batches of 2
"""
from numpy.ma.core import append
from torchvision import message

RAW_LINES = [
    "2026-02-28 10:00:01 | INFO  | Application started",
    "2026-02-28 10:00:02 | DEBUG | Loading config from /etc/app.conf",
    "2026-02-28 10:00:03 | ERROR | Failed to connect to database",
    "2026-02-28 10:00:04 | INFO  | Retrying connection...",
    "2026-02-28 10:00:05 | ERROR | Timeout waiting for DB response",
    "2026-02-28 10:00:06 | WARN  | Using fallback cache",
    "2026-02-28 10:00:07 | INFO  | Request served in 120ms",
    "2026-02-28 10:00:08 | ERROR | Disk space critically low",
    "2026-02-28 10:00:09 | INFO  | Cleanup job started",
    "2026-02-28 10:00:10 | ERROR | Permission denied: /var/log/app.log",
    "2026-02-28 10:00:11 | INFO  | Shutdown signal received",
]


def read_logs(lines):

    for line in lines:
        yield line.strip()


def parse_log(lines):
    for line in lines:
        parts = line.split(' | ')
        if len(parts) == 3:
            timestamp, level, message = parts

            yield {
                'timestamp': timestamp.strip(),
                'level': level.strip(),
                'message': message.strip()
            }
def filter_by_level(logs, level: str):
    for log in logs:

        if log['level'] == level:
            yield log


def batch(items, size: int):

    batch_list = []
    for item in items:
        batch_list.append(item)
        if len(batch_list) == size:
            yield batch_list
            batch_list = []
    if batch_list:
        yield batch_list

# ---------------------------------------------------------------------------
# Run & verify
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Build the pipeline: read -> parse -> filter ERRORs -> batch by 2
    raw = read_logs(RAW_LINES)
    parsed = parse_log(raw)
    errors = filter_by_level(parsed, "ERROR")
    batches = batch(errors, size=2)

    print("=== ERROR logs in batches of 2 ===")
    for i, group in enumerate(batches, 1):
        print(f"\nBatch {i}:")
        for log in group:
            print(f"  [{log['timestamp']}] {log['message']}")

