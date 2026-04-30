// @vitest-environment node
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { pathToFileURL } from "node:url";
import { vi } from "vitest";
import { resolveSparkRuntime } from "./spark-runtime";
import { launchHistoryServer } from "./history-server";

test("launchHistoryServer spawns spark-class with the expected environment", () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "spark-ui-service-history-"));
  const repoRoot = path.join(tempRoot, "repo");
  const sparkHome = path.join(tempRoot, "spark-home");
  const workspaceDir = path.join(tempRoot, "task workspace");
  const eventLogDir = path.join(workspaceDir, "event logs");
  const uimetaDir = path.join(workspaceDir, "ui meta");
  const logFile = path.join(workspaceDir, "logs", "history-server.log");
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

  launchHistoryServer(runtime, {
    eventLogDir,
    uimetaDir,
    port: 18080,
    logFile,
    spawnImpl: spawn
  });

  expect(spawn).toHaveBeenCalledWith(
    runtime.sparkClass,
    ["org.apache.spark.deploy.history.HistoryServer"],
    expect.objectContaining({
      stdio: ["ignore", "pipe", "pipe"]
    })
  );

  const spawnCall = vi.mocked(spawn).mock.calls[0];
  const env = spawnCall[2]?.env as Record<string, string>;
  expect(env.SPARK_DAEMON_CLASSPATH).toBe(runtime.projectJar);
  expect(env.SPARK_HISTORY_OPTS).toContain(
    `spark.history.fs.logDirectory=${pathToFileURL(eventLogDir).toString()}`
  );
  expect(env.SPARK_HISTORY_OPTS).toContain(
    "spark.history.provider=org.apache.spark.deploy.history.UIMetaProvider"
  );
  expect(env.SPARK_HISTORY_OPTS).toContain(`spark.uimeta.dir=${pathToFileURL(uimetaDir).toString()}`);
  expect(env.SPARK_HISTORY_OPTS).toContain("spark.history.ui.port=18080");
  expect(writeStreamSpy).toHaveBeenCalledWith(logFile, expect.any(Object));

  writeStreamSpy.mockRestore();
});
