#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
API_URL="${API_URL:-http://127.0.0.1:3000}"
EVENT_LOG_DIR="${EVENT_LOG_DIR:-/tmp/spark-events}"
TASK_REPORT_FILE="${TASK_REPORT_FILE:-$ROOT_DIR/benchmark-1gb-web-task.json}"
SPARK_HOME="${SPARK_HOME:-/opt/spark}"
SPARK_SUBMIT="${SPARK_SUBMIT:-$SPARK_HOME/bin/spark-submit}"
MVN_BIN="${MVN_BIN:-$ROOT_DIR/apache-maven-3.9.5/bin/mvn}"
TARGET_JAR="${TARGET_JAR:-$ROOT_DIR/target/spark-uiservice-1.0-SNAPSHOT.jar}"

if [[ ! -x "$SPARK_SUBMIT" ]]; then
  echo "spark-submit not found at $SPARK_SUBMIT" >&2
  exit 1
fi

mkdir -p "$EVENT_LOG_DIR"

"$MVN_BIN" -DskipTests package

"$SPARK_SUBMIT" \
  --conf "spark.eventLog.enabled=true" \
  --conf "spark.eventLog.compress=false" \
  --conf "spark.eventLog.dir=file://$EVENT_LOG_DIR" \
  --class org.apache.spark.deploy.history.LargeEventLogGenerator \
  "$TARGET_JAR" \
  --target-eventlog-gb 1 \
  --tasks-per-job 2048 \
  --records-per-task 1 \
  --progress-every-jobs 1 \
  --sql-every-jobs 1 \
  --sql-queries-per-batch 1 \
  --max-jobs 128

EVENT_LOG_PATH="$(find "$EVENT_LOG_DIR" -type f ! -name '*.inprogress' -print | sort | tail -n 1)"
if [[ -z "$EVENT_LOG_PATH" ]]; then
  echo "No eventlog was generated under $EVENT_LOG_DIR" >&2
  exit 1
fi

TASK_ID="$(
  curl -fsS -X POST "$API_URL/api/tasks" \
    -H 'Content-Type: application/json' \
    -d "{\"inputMode\":\"path\",\"eventLogPath\":\"$EVENT_LOG_PATH\",\"outputMode\":\"managed\"}" \
  | node -e 'const fs = require("fs"); const body = JSON.parse(fs.readFileSync(0, "utf8")); process.stdout.write(body.task.id);'
)"

while true; do
  TASK_JSON="$(curl -fsS "$API_URL/api/tasks/$TASK_ID")"
  STATUS="$(printf '%s' "$TASK_JSON" | node -e 'const fs = require("fs"); const body = JSON.parse(fs.readFileSync(0, "utf8")); process.stdout.write(body.task.status);')"

  if [[ "$STATUS" == "ready" ]]; then
    printf '%s\n' "$TASK_JSON" | tee "$TASK_REPORT_FILE" >/dev/null
    echo "Task $TASK_ID is ready. Report written to $TASK_REPORT_FILE"
    exit 0
  fi

  if [[ "$STATUS" == "failed" ]]; then
    printf '%s\n' "$TASK_JSON" | tee "$TASK_REPORT_FILE" >/dev/null
    echo "Task $TASK_ID failed. Report written to $TASK_REPORT_FILE" >&2
    exit 1
  fi

  sleep 5
done
