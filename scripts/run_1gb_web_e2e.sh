#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
API_URL="${API_URL:-http://127.0.0.1:3000}"
EVENT_LOG_DIR="${EVENT_LOG_DIR:-/tmp/spark-events}"
UIMETA_DIR="${UIMETA_DIR:-/tmp/spark-uimeta}"
TASK_EVENT_LOG_DIR="${TASK_EVENT_LOG_DIR:-$EVENT_LOG_DIR}"
TASK_EVENT_LOG_PATH="${TASK_EVENT_LOG_PATH:-}"
TASK_REPORT_FILE="${TASK_REPORT_FILE:-$ROOT_DIR/benchmark-1gb-web-task.json}"
SPARK_HOME="${SPARK_HOME:-/opt/spark}"
SPARK_SUBMIT="${SPARK_SUBMIT:-}"
MVN_BIN="${MVN_BIN:-$ROOT_DIR/apache-maven-3.9.5/bin/mvn}"
APP_JAR="${APP_JAR:-$ROOT_DIR/target/spark-uiservice-tests.jar}"
NUM_JOBS="${NUM_JOBS:-300}"
TASKS_PER_JOB="${TASKS_PER_JOB:-500}"
SPARK_IMAGE_PREPARE_SCRIPT="${SPARK_IMAGE_PREPARE_SCRIPT:-$ROOT_DIR/scripts/prepare_spark_runtime_from_image.sh}"
SPARK_IMAGE="${SPARK_IMAGE:-}"
SPARK_IMAGE_SPARK_HOME="${SPARK_IMAGE_SPARK_HOME:-/opt/spark}"

json_read_task_field() {
  local field="$1"

  if command -v node >/dev/null 2>&1; then
    node -e "const fs = require('fs'); const body = JSON.parse(fs.readFileSync(0, 'utf8')); process.stdout.write(String(body.task['$field'] ?? ''));"
    return
  fi

  if command -v python3 >/dev/null 2>&1; then
    python3 -c "import json, sys; print(json.load(sys.stdin)['task'].get('$field', ''), end='')"
    return
  fi

  if command -v perl >/dev/null 2>&1; then
    perl -MJSON::PP -e '
      my $field = shift @ARGV;
      local $/;
      my $body = decode_json(<>);
      print defined $body->{task}{$field} ? $body->{task}{$field} : q{};
    ' "$field"
    return
  fi

  echo "Neither node, python3, nor perl is available to parse JSON." >&2
  exit 1
}

build_generator_jar() {
  "$MVN_BIN" -DskipTests test-compile package

  if [[ ! -f "$APP_JAR" ]]; then
    jar cf "$APP_JAR" -C "$ROOT_DIR/target/classes" . -C "$ROOT_DIR/target/test-classes" .
  fi

  if [[ ! -f "$APP_JAR" ]]; then
    echo "Generator jar not found at $APP_JAR" >&2
    exit 1
  fi
}

detect_spark_image() {
  if [[ -n "$SPARK_IMAGE" ]]; then
    printf '%s\n' "$SPARK_IMAGE"
    return 0
  fi

  if [[ -x "$SPARK_IMAGE_PREPARE_SCRIPT" ]]; then
    "$SPARK_IMAGE_PREPARE_SCRIPT" --image-only
    return 0
  fi

  return 1
}

run_generator_with_host_spark() {
  "$SPARK_SUBMIT" \
    --master "local[*]" \
    --driver-memory 6g \
    --conf "spark.eventLog.enabled=true" \
    --conf "spark.eventLog.compress=false" \
    --conf "spark.eventLog.dir=file://$EVENT_LOG_DIR" \
    --conf "spark.extraListeners=org.apache.spark.deploy.history.UIMetaLoggingListener" \
    --conf "spark.uimeta.dir=file://$UIMETA_DIR" \
    --class org.apache.spark.deploy.history.LargeEventLogGenerator \
    "$APP_JAR" \
    "$NUM_JOBS" "$TASKS_PER_JOB"
}

run_generator_with_spark_image() {
  local image="$1"
  local app_jar_dir app_jar_name

  if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required to use a local Spark image runtime." >&2
    exit 1
  fi

  if ! docker image inspect "$image" >/dev/null 2>&1; then
    echo "Spark image $image is not present locally." >&2
    exit 1
  fi

  app_jar_dir="$(cd "$(dirname "$APP_JAR")" && pwd)"
  app_jar_name="$(basename "$APP_JAR")"

  docker run --rm \
    -v "$app_jar_dir:/app-jar" \
    -v "$EVENT_LOG_DIR:/tmp/spark-events" \
    -v "$UIMETA_DIR:/tmp/spark-uimeta" \
    "$image" \
    bash -lc "\
      $SPARK_IMAGE_SPARK_HOME/bin/spark-submit \
        --master 'local[*]' \
        --driver-memory 6g \
        --conf spark.eventLog.enabled=true \
        --conf spark.eventLog.compress=false \
        --conf spark.eventLog.dir=file:///tmp/spark-events \
        --conf spark.extraListeners=org.apache.spark.deploy.history.UIMetaLoggingListener \
        --conf spark.uimeta.dir=file:///tmp/spark-uimeta \
        --class org.apache.spark.deploy.history.LargeEventLogGenerator \
        /app-jar/$app_jar_name \
        $NUM_JOBS $TASKS_PER_JOB"
}

ensure_spark_submit() {
  if [[ -n "$SPARK_SUBMIT" && -x "$SPARK_SUBMIT" ]]; then
    return 0
  fi

  if [[ -x "$SPARK_HOME/bin/spark-submit" ]]; then
    SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
    return 0
  fi

  return 1
}

mkdir -p "$EVENT_LOG_DIR" "$UIMETA_DIR"

build_generator_jar

if ensure_spark_submit; then
  run_generator_with_host_spark
else
  SPARK_IMAGE="$(detect_spark_image)"
  run_generator_with_spark_image "$SPARK_IMAGE"
fi

EVENT_LOG_PATH="$(find "$EVENT_LOG_DIR" -type f ! -name '*.inprogress' -print | sort | tail -n 1)"
if [[ -z "$EVENT_LOG_PATH" ]]; then
  echo "No eventlog was generated under $EVENT_LOG_DIR" >&2
  exit 1
fi

if [[ -z "$TASK_EVENT_LOG_PATH" ]]; then
  TASK_EVENT_LOG_PATH="$TASK_EVENT_LOG_DIR/$(basename "$EVENT_LOG_PATH")"
fi

TASK_CREATE_RESPONSE="$(
  curl -fsS -X POST "$API_URL/api/tasks" \
    -H 'Content-Type: application/json' \
    -d "{\"inputMode\":\"path\",\"eventLogPath\":\"$TASK_EVENT_LOG_PATH\",\"outputMode\":\"managed\"}"
)" || {
  echo "Failed to create a web task via $API_URL/api/tasks" >&2
  exit 1
}

TASK_ID="$(printf '%s' "$TASK_CREATE_RESPONSE" | json_read_task_field id)"

while true; do
  TASK_JSON="$(curl -fsS "$API_URL/api/tasks/$TASK_ID")" || {
    echo "Failed to fetch task $TASK_ID from $API_URL/api/tasks/$TASK_ID" >&2
    exit 1
  }
  STATUS="$(printf '%s' "$TASK_JSON" | json_read_task_field status)"

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
