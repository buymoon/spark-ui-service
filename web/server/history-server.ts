import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { spawn, type ChildProcess } from "node:child_process";
import type { SparkRuntime } from "./spark-runtime";

export interface HistoryServerLaunchInput {
  eventLogDir: string;
  uimetaDir: string;
  port: number;
  logFile: string;
  spawnImpl?: typeof spawn;
}

export function launchHistoryServer(
  runtime: SparkRuntime,
  input: HistoryServerLaunchInput
): ChildProcess {
  fs.mkdirSync(path.dirname(input.logFile), { recursive: true });
  fs.mkdirSync(input.eventLogDir, { recursive: true });
  fs.mkdirSync(input.uimetaDir, { recursive: true });

  const logStream = fs.createWriteStream(input.logFile, { flags: "a" });
  const spawnImpl = input.spawnImpl ?? spawn;
  const child = spawnImpl(runtime.sparkClass, ["org.apache.spark.deploy.history.HistoryServer"], {
    stdio: ["ignore", "pipe", "pipe"],
    env: {
      ...process.env,
      SPARK_DAEMON_CLASSPATH: runtime.projectJar,
      SPARK_HISTORY_OPTS: [
        `-Dspark.history.fs.logDirectory=${pathToFileURL(input.eventLogDir).toString()}`,
        "-Dspark.history.provider=org.apache.spark.deploy.history.UIMetaProvider",
        `-Dspark.uimeta.dir=${pathToFileURL(input.uimetaDir).toString()}`,
        `-Dspark.history.ui.port=${input.port}`
      ].join(" ")
    }
  });

  pipeProcessLogs(child, logStream);
  return child;
}

function pipeProcessLogs(child: ChildProcess, logStream: fs.WriteStream) {
  child.stdout?.pipe(logStream, { end: false });
  child.stderr?.pipe(logStream, { end: false });
  child.once("close", () => {
    logStream.end();
  });
}

export { resolveSparkRuntime } from "./spark-runtime";
