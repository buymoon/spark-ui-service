#!/bin/bash
set -e

echo "========================================"
echo " Spark UIService Performance Test Runner"
echo "========================================"

# Download Spark 3.3.0 if not present
SPARK_HOME=/opt/spark-3.3.0-bin-hadoop3
if [ ! -d "$SPARK_HOME" ]; then
  echo "[1/4] Downloading Spark 3.3.0..."
  curl -sL https://archive.apache.org/dist/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz | tar -xzf - -C /opt
fi
export SPARK_HOME
export PATH=$SPARK_HOME/bin:$PATH

echo "[2/4] Running LargeEventLogGenerator (1500 jobs x 500 tasks = 750k tasks, target ~5GB event log)..."
mkdir -p /tmp/spark-events /tmp/spark-uimeta

MAIN_JAR=/app/target/spark-uiservice-1.0-SNAPSHOT.jar
TEST_CLASSES=/app/target/test-classes

spark-submit \
  --master "local[*]" \
  --driver-memory 6g \
  --driver-class-path "$TEST_CLASSES:$MAIN_JAR" \
  --class org.apache.spark.deploy.history.LargeEventLogGenerator \
  --conf "spark.extraListeners=org.apache.spark.deploy.history.UIMetaLoggingListener" \
  --conf "spark.uimeta.dir=file:///tmp/spark-uimeta" \
  --conf "spark.eventLog.enabled=true" \
  --conf "spark.eventLog.dir=file:///tmp/spark-events" \
  $MAIN_JAR \
  1500 500

echo ""
echo "[3/4] File sizes after generation:"
echo "  Event logs:"
ls -lh /tmp/spark-events/ 2>/dev/null || echo "  (none)"
echo "  UIMeta files:"
ls -lh /tmp/spark-uimeta/ 2>/dev/null || echo "  (none)"

echo ""
echo "[4/4] Running ReadPerformanceTest..."
APP_ID=$(ls /tmp/spark-events/ | head -1)
echo "  Using App ID: $APP_ID"

spark-submit \
  --master "local[*]" \
  --driver-memory 6g \
  --driver-class-path "$TEST_CLASSES:$MAIN_JAR" \
  --class org.apache.spark.deploy.history.ReadPerformanceTest \
  --conf "spark.history.fs.logDirectory=file:///tmp/spark-events" \
  --conf "spark.uimeta.dir=file:///tmp/spark-uimeta" \
  $MAIN_JAR \
  "$APP_ID"

echo ""
echo "Done!"
