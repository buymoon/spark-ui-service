# Spark UI Service

Spark UI Service is a Spark History Server extension that accelerates UI loading by serving compact metadata snapshots instead of replaying full event logs.

A lightweight extension for Apache Spark History Server that replaces full event log replay with compact `.uimeta` snapshot files, improving Spark UI loading speed while reducing storage and read overhead for large-scale history data.

## Why This Project Exists

The default Spark History Server relies on `FsHistoryProvider` to scan and replay event logs. For large Spark applications, that approach often leads to:

- Slow UI startup and expensive first-load latency
- Large event log storage footprint
- Higher server pressure under concurrent access
- Amplified read latency in cloud or remote storage environments

This project introduces a lighter metadata path: generate `.uimeta` snapshots during job execution, then let the History Server reconstruct UI state from those snapshots. If a snapshot is unavailable or invalid, the implementation falls back to the standard `FsHistoryProvider`.

## Features

- Compact `.uimeta` snapshot format for Spark UI metadata
- `UIMetaProvider` for on-demand UI loading without full log-directory scanning
- `UIMetaLoggingListener` to persist snapshots during job execution
- Better storage efficiency and lower read latency for large event logs
- Automatic fallback to `FsHistoryProvider` when snapshots are missing or unreadable
- Local test, benchmark, and Docker-based validation support

## How It Works

```text
Spark Application
  -> UIMetaLoggingListener
  -> AppStatusStore / KVStore snapshot
  -> *.uimeta

Spark History Server
  -> UIMetaProvider
  -> read *.uimeta
  -> reconstruct AppStatusStore
  -> build SparkUI
```

### Core Components

- `UIMetaProvider`
  - A custom `ApplicationHistoryProvider` for the History Server
  - Reads `.uimeta` files first to rebuild Spark UI metadata
  - Falls back to `FsHistoryProvider` if snapshot loading fails
- `UIMetaLoggingListener`
  - A driver-side `SparkListener`
  - Writes snapshots when stages complete, jobs end, and the application ends
- `UIMetaFile`
  - Utility for reading and writing `.uimeta` files
  - Uses a simple binary layout with a magic header, class name, and JSON bytes

## Tech Stack

- Scala 2.12.15
- Apache Spark 3.3.0
- Maven
- Java 8
- ScalaTest + JUnit
- Docker / Docker Compose for local verification

## Project Structure

```text
.
├── src/main/scala/org/apache/spark/deploy/history
│   ├── UIMetaFile.scala
│   ├── UIMetaLoggingListener.scala
│   └── UIMetaProvider.scala
├── src/test/scala/org/apache/spark/deploy/history
│   ├── LargeEventLogGenerator.scala
│   ├── ReadPerformanceTest.scala
│   └── UIMetaFileSuite.scala
├── docker/
│   └── Dockerfile.test
├── docker-compose.yml
└── pom.xml
```

## Getting Started

### Prerequisites

- JDK 8
- Maven 3.x
- Apache Spark 3.3.0 runtime environment

The repository already includes `apache-maven-3.9.5/`, so you can use the bundled Maven binary if Maven is not installed globally.

### Build

```bash
mvn clean package
```

Or use the Maven binary shipped in this repository:

```bash
./apache-maven-3.9.5/bin/mvn clean package
```

The packaged JAR is generated at `target/spark-uiservice-1.0-SNAPSHOT.jar`.

### Run Tests

```bash
mvn test
```

## Local Web Tool

The repository now includes a local single-machine web workflow under `web/` for:

- creating a task from an uploaded eventlog or a local absolute path
- preprocessing a large Spark eventlog into a reusable `.uimeta`
- launching a task-scoped Spark History Server
- reopening the native Spark History UI without replaying the eventlog every time

### Start the local backend

Make sure `SPARK_HOME` points at a Spark 3.3 runtime and that the project jar has been built:

```bash
./apache-maven-3.9.5/bin/mvn -DskipTests package
cd web
SPARK_HOME=/opt/spark npm run dev:server
```

The backend listens on port `3000` by default.

### Reuse a local Spark Docker image

If you already have a local Spark image, `scripts/prepare_spark_runtime_from_image.sh` can extract a matching Spark + Java runtime to the host filesystem so that you can mount it into Linux containers:

```bash
eval "$(bash scripts/prepare_spark_runtime_from_image.sh)"
printf 'SPARK_HOME=%s\nJAVA_HOME=%s\n' "$SPARK_HOME" "$JAVA_HOME"
```

By default the helper script:

- inspects local `apache/spark:*` images only and does not pull anything
- prefers an image matching the repository `spark.version` from `pom.xml`
- falls back to the first local `apache/spark:*` image if no exact family match exists

You can force a specific local tag or custom runtime paths inside the image:

```bash
SPARK_IMAGE=apache/spark:3.5.3-scala2.12-java17-ubuntu \
SPARK_IMAGE_SPARK_HOME=/opt/spark \
SPARK_IMAGE_JAVA_HOME=/opt/java/openjdk \
bash scripts/prepare_spark_runtime_from_image.sh
```

Note:

- the extracted runtime contains Linux binaries, so it is meant for container mounts rather than direct macOS execution
- the helper only inspects local images; it does not pull missing tags

### Start the frontend

In a second terminal:

```bash
cd web
npm run dev:client
```

Open the Vite URL in your browser, create a task, and then use the generated “Open Spark UI” action to jump into the native History Server page.

### Run the 1 GB local web flow

After the backend is running, you can drive the path-mode API end-to-end with:

```bash
bash scripts/run_1gb_web_e2e.sh
```

If `SPARK_HOME` is missing, the script will:

- build the generator test jar locally
- detect a local `apache/spark:*` image, or use `SPARK_IMAGE` if you set one
- run `LargeEventLogGenerator` inside that local Spark image
- write the eventlog and `.uimeta` back to the host via bind mounts

That means the 1 GB generator path works even when the host does not have a native Spark installation.

If your `web` backend runs inside a container, set `TASK_EVENT_LOG_DIR` to the eventlog path as seen from inside that backend container. Example:

```bash
EVENT_LOG_DIR="$PWD/.e2e-spark-events/smoke" \
TASK_EVENT_LOG_DIR=/tmp/spark-events/smoke \
bash scripts/run_1gb_web_e2e.sh
```

In that setup:

- `EVENT_LOG_DIR` is where the generator writes on the host
- `TASK_EVENT_LOG_DIR` is the absolute path the backend can actually read

The script will:

- build the project jar
- generate an open-source Spark eventlog targeting `1 GB`
- submit that eventlog to the local web backend in path mode
- poll until the task becomes `ready`
- write the final task payload to `benchmark-1gb-web-task.json`

## Enable It In Spark

### 1. Generate `.uimeta` snapshots

When submitting a Spark application, add the custom listener on the driver side:

```bash
spark-submit \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///tmp/spark-events \
  --conf spark.extraListeners=org.apache.spark.deploy.history.UIMetaLoggingListener \
  --conf spark.uimeta.dir=file:///tmp/spark-uimeta \
  --jars target/spark-uiservice-1.0-SNAPSHOT.jar \
  ...
```

### 2. Start History Server with `UIMetaProvider`

```bash
spark-class org.apache.spark.deploy.history.HistoryServer \
  --properties-file /dev/null \
  --conf spark.history.fs.logDirectory=file:///tmp/spark-events \
  --conf spark.history.provider=org.apache.spark.deploy.history.UIMetaProvider \
  --conf spark.uimeta.dir=file:///tmp/spark-uimeta
```

Notes:

- `spark.history.provider` must point to the implementation in this repository
- `spark.uimeta.dir` is the directory used to read and write `.uimeta` files
- If a `.uimeta` file is missing or cannot be loaded, the service falls back to Spark's default history provider

## Run With Docker

The repository includes `docker-compose.yml` and `docker/Dockerfile.test` for local validation:

```bash
docker compose up --build
```

Recommended preparation steps:

1. Run `mvn clean package`
2. Make sure `target/spark-uiservice-1.0-SNAPSHOT.jar` exists
3. Prepare local `spark-events/` and `spark-uimeta/` directories for bind mounts

After startup, Spark History Server is available at [http://localhost:18080](http://localhost:18080).

## Benchmarking

The repository includes two helper programs for testing and benchmarking:

- `LargeEventLogGenerator`
  - Generates many Spark jobs and SQL queries to produce larger event logs and `.uimeta` files
- `ReadPerformanceTest`
  - Compares load time and file size between `FsHistoryProvider` and `UIMetaProvider`

### Generate test data

```bash
spark-submit \
  --class org.apache.spark.deploy.history.LargeEventLogGenerator \
  target/spark-uiservice-1.0-SNAPSHOT.jar \
  100 1000
```

### Run the performance comparison

```bash
spark-submit \
  --class org.apache.spark.deploy.history.ReadPerformanceTest \
  target/spark-uiservice-1.0-SNAPSHOT.jar \
  <appId>
```

If `appId` is omitted, the program uses the most recent event log under `/tmp/spark-events`.

## Development Notes

### `.uimeta` file format

The current implementation uses a compact binary layout:

```text
[magic "UI_S"]
repeated:
  [class name length]
  [class name bytes]
  [data length]
  [json data bytes]
```

Snapshot data is mainly taken from key Spark UI state objects in `AppStatusStore` / `KVStore`, including:

- Job / Stage / Task metadata
- Executor summaries
- Application info
- SQL UI objects when available in the runtime environment

### Current implementation behavior

- `getListing()` returns an empty iterator to avoid expensive directory scans
- UI data is loaded on demand by `appId`
- Snapshot writing uses a temporary file plus rename flow to reduce partial-write risk
- Running tasks are skipped during snapshotting to reduce noise and duplicate data

## Good Fit For

- Large Spark history logs where History Server startup is too slow
- Scenarios that need lower Spark UI reconstruction cost
- Cloud or remote filesystem deployments where log reads are expensive
- Performance experiments around Spark History Server internals

## Known Limitations

- This project is currently closer to a prototype / performance experiment than a full replacement for Spark History Server
- `.uimeta` content depends on Spark internal status classes, so cross-version compatibility needs extra validation
- `getListing()` does not implement full application discovery, so the provider is best suited for targeted access by application ID
- Full value requires integration on both the Spark job side and the History Server side

## Future Improvements

- Incremental snapshots and compression strategies
- Better application listing and indexing support
- Stronger support for object storage and caching layers
- More comprehensive compatibility and failure-handling tests
- More systematic benchmark datasets and published results

## License

This project is licensed under the Apache License 2.0. See [LICENSE](./LICENSE) for details.
