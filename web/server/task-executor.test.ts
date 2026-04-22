// @vitest-environment node
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { vi } from "vitest";
import { resolveSparkRuntime } from "./spark-runtime";
import { launchEventLogPreprocessor } from "./task-executor";

test("resolveSparkRuntime discovers spark binaries and the built project jar", () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "spark-ui-service-runtime-"));
  const repoRoot = path.join(tempRoot, "repo");
  const sparkHome = path.join(tempRoot, "spark-home");
  fs.mkdirSync(path.join(repoRoot, "target"), { recursive: true });
  fs.mkdirSync(path.join(sparkHome, "bin"), { recursive: true });
  fs.writeFileSync(path.join(repoRoot, "target", "spark-uiservice-1.0-SNAPSHOT.jar"), "");
  fs.writeFileSync(path.join(sparkHome, "bin", "spark-submit"), "");
  fs.writeFileSync(path.join(sparkHome, "bin", "spark-class"), "");

  const runtime = resolveSparkRuntime({ repoRoot, sparkHome });

  expect(runtime).toMatchObject({
    repoRoot,
    sparkHome,
    sparkSubmit: path.join(sparkHome, "bin", "spark-submit"),
    sparkClass: path.join(sparkHome, "bin", "spark-class"),
    projectJar: path.join(repoRoot, "target", "spark-uiservice-1.0-SNAPSHOT.jar")
  });
});

test("resolveSparkRuntime throws a clear error when the project jar is missing", () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "spark-ui-service-runtime-"));
  const repoRoot = path.join(tempRoot, "repo");
  const sparkHome = path.join(tempRoot, "spark-home");
  fs.mkdirSync(path.join(sparkHome, "bin"), { recursive: true });
  fs.writeFileSync(path.join(sparkHome, "bin", "spark-submit"), "");
  fs.writeFileSync(path.join(sparkHome, "bin", "spark-class"), "");

  expect(() => resolveSparkRuntime({ repoRoot, sparkHome })).toThrow(/built project jar/i);
});

test("launchEventLogPreprocessor spawns spark-submit with the offline preprocessor arguments", () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "spark-ui-service-preprocess-"));
  const repoRoot = path.join(tempRoot, "repo");
  const sparkHome = path.join(tempRoot, "spark-home");
  const workspaceDir = path.join(tempRoot, "task-workspace");
  const logFile = path.join(workspaceDir, "logs", "preprocessor.log");
  const eventLogPath = path.join(tempRoot, "input.eventlog");
  const uimetaDir = path.join(workspaceDir, "uimeta");
  const resultFile = path.join(workspaceDir, "results", "preprocessor.json");
  fs.mkdirSync(path.join(repoRoot, "target"), { recursive: true });
  fs.mkdirSync(path.join(sparkHome, "bin"), { recursive: true });
  fs.mkdirSync(workspaceDir, { recursive: true });
  fs.writeFileSync(path.join(repoRoot, "target", "spark-uiservice-1.0-SNAPSHOT.jar"), "");
  fs.writeFileSync(path.join(sparkHome, "bin", "spark-submit"), "");
  fs.writeFileSync(path.join(sparkHome, "bin", "spark-class"), "");

  const runtime = resolveSparkRuntime({ repoRoot, sparkHome });
  const spawn = vi.fn(() => ({
    stdout: new PassThrough(),
    stderr: new PassThrough(),
    on: vi.fn(),
    once: vi.fn(),
    kill: vi.fn()
  })) as unknown as typeof import("node:child_process").spawn;
  const writeStreamSpy = vi.spyOn(fs, "createWriteStream");

  launchEventLogPreprocessor(runtime, {
    eventLogPath,
    uimetaDir,
    resultFile,
    logFile,
    spawnImpl: spawn
  });

  expect(spawn).toHaveBeenCalledWith(
    runtime.sparkSubmit,
    [
      "--class",
      "org.apache.spark.deploy.history.EventLogPreprocessor",
      runtime.projectJar,
      "--eventlog",
      eventLogPath,
      "--uimeta-dir",
      uimetaDir,
      "--result-file",
      resultFile
    ],
    expect.objectContaining({
      stdio: ["ignore", "pipe", "pipe"]
    })
  );
  expect(writeStreamSpy).toHaveBeenCalledWith(logFile, expect.any(Object));

  writeStreamSpy.mockRestore();
});
