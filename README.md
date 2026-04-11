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
