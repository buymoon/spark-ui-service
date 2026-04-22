import fs from "node:fs";
import path from "node:path";
import { spawn, type ChildProcess, type SpawnOptions } from "node:child_process";
import type { SparkRuntime } from "./spark-runtime";

export interface EventLogPreprocessorLaunchInput {
  eventLogPath: string;
  uimetaDir: string;
  resultFile: string;
  logFile: string;
  spawnImpl?: typeof spawn;
}

export function launchEventLogPreprocessor(
  runtime: SparkRuntime,
  input: EventLogPreprocessorLaunchInput
): ChildProcess {
  fs.mkdirSync(path.dirname(input.logFile), { recursive: true });
  fs.mkdirSync(path.dirname(input.resultFile), { recursive: true });
  fs.mkdirSync(input.uimetaDir, { recursive: true });

  const logStream = fs.createWriteStream(input.logFile, { flags: "a" });
  const spawnImpl = input.spawnImpl ?? spawn;
  const child = spawnImpl(
    runtime.sparkSubmit,
    [
      "--class",
      "org.apache.spark.deploy.history.EventLogPreprocessor",
      runtime.projectJar,
      "--eventlog",
      input.eventLogPath,
      "--uimeta-dir",
      input.uimetaDir,
      "--result-file",
      input.resultFile
    ],
    {
      stdio: ["ignore", "pipe", "pipe"]
    }
  );

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
