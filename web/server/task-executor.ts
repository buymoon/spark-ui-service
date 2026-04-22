import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import { spawn, type ChildProcess } from "node:child_process";
import { launchHistoryServer } from "./history-server";
import { TaskStore } from "./task-store";
import { resolveSparkRuntime, type SparkRuntime } from "./spark-runtime";
import type { TaskExecutionResult, TaskExecutor, TaskRecord } from "./types";

export interface EventLogPreprocessorLaunchInput {
  eventLogPath: string;
  uimetaDir: string;
  resultFile: string;
  logFile: string;
  spawnImpl?: typeof spawn;
}

interface CreateTaskExecutorInput {
  workspaceRoot: string;
  repoRoot: string;
  sparkHome?: string;
  baseHistoryPort?: number;
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
  const args = buildEventLogPreprocessorArgs(runtime, input);
  const child = spawnImpl(
    runtime.sparkSubmit,
    args,
    {
      stdio: ["ignore", "pipe", "pipe"]
    }
  );

  pipeProcessLogs(child, logStream);
  return child;
}

export function createTaskExecutor(input: CreateTaskExecutorInput): TaskExecutor {
  const runtime = resolveSparkRuntime({ repoRoot: input.repoRoot, sparkHome: input.sparkHome });
  const store = new TaskStore(input.workspaceRoot);
  const runningProcesses = new Map<string, { preprocessor?: ChildProcess; historyServer?: ChildProcess }>();
  let nextHistoryPort = input.baseHistoryPort ?? 18080;

  return {
    enqueue: async (taskId: string) => {
      stopProcesses(taskId);

      const task = store.get(taskId);
      if (!task?.sourcePath) {
        throw new Error(`Task ${taskId} is missing a source eventlog path.`);
      }
      const runToken = task.runToken;

      const historyDir = path.join(task.workspaceDir, "history");
      const uimetaDir = path.join(task.workspaceDir, "uimeta");
      const eventsDir = path.join(task.workspaceDir, "events");
      const resultFile = path.join(historyDir, "preprocessor-result.json");
      const preprocessorLogFile = path.join(historyDir, "preprocessor.log");
      const historyServerLogFile = path.join(historyDir, "history-server.log");
      const executionStartedAt = new Date().toISOString();

      const preprocessor = launchEventLogPreprocessor(runtime, {
        eventLogPath: task.sourcePath,
        uimetaDir,
        resultFile,
        logFile: preprocessorLogFile
      });
      updateTaskRuntime(taskId, runToken, {
        executionStartedAt,
        preprocessorPid: preprocessor.pid ?? undefined,
        preprocessorLogPath: preprocessorLogFile,
        historyServerLogPath: historyServerLogFile,
        preprocessorCommand: [runtime.sparkSubmit, ...buildEventLogPreprocessorArgs(runtime, {
          eventLogPath: task.sourcePath,
          uimetaDir,
          resultFile,
          logFile: preprocessorLogFile
        })],
        historyServerState: "preprocessing"
      });
      runningProcesses.set(taskId, { preprocessor });

      const preprocessorExitCode = await waitForExit(preprocessor, "EventLogPreprocessor");
      if (preprocessorExitCode !== 0) {
        updateTaskRuntime(taskId, runToken, { historyServerState: "failed" });
        throw new Error(`EventLogPreprocessor exited with code ${preprocessorExitCode}.`);
      }

      const result = JSON.parse(fs.readFileSync(resultFile, "utf8")) as {
        appId: string;
        attemptId?: string;
        uimetaPath?: string;
      };
      prepareEventLogDirectory(task, eventsDir);

      const historyPort = await reserveHistoryPort(nextHistoryPort);
      nextHistoryPort = historyPort + 1;
      updateTaskRuntime(taskId, runToken, { historyPort });
      const historyServerInput = {
        eventLogDir: eventsDir,
        uimetaDir,
        port: historyPort,
        logFile: historyServerLogFile
      };
      const historyServer = launchHistoryServer(runtime, historyServerInput);
      updateTaskRuntime(taskId, runToken, {
        historyPort,
        historyServerPid: historyServer.pid ?? undefined,
        historyServerLogPath: historyServerLogFile,
        historyServerCommand: [runtime.sparkClass, "org.apache.spark.deploy.history.HistoryServer"],
        historyServerState: "starting"
      });
      runningProcesses.set(taskId, { historyServer });
      try {
        await waitForPortReady(historyPort, 30000);
      } catch (error) {
        updateTaskRuntime(taskId, runToken, { historyServerState: "failed" });
        historyServer.kill("SIGTERM");
        runningProcesses.delete(taskId);
        throw error;
      }
      updateTaskRuntime(taskId, runToken, { historyServerState: "ready" });

      return {
        status: "ready",
        appId: result.appId,
        attemptId: result.attemptId,
        uimetaPath: result.uimetaPath,
        historyPort,
        historyUrl: `http://127.0.0.1:${historyPort}/history/${result.appId}/jobs/`
      };
    },
    stop: async (taskId: string) => {
      stopProcesses(taskId);
      const task = store.get(taskId);
      if (task) {
        store.update(taskId, { historyServerState: "stopped" });
      }
    }
  };

  function stopProcesses(taskId: string) {
    const running = runningProcesses.get(taskId);
    running?.historyServer?.kill("SIGTERM");
    running?.preprocessor?.kill("SIGTERM");
    runningProcesses.delete(taskId);
  }

  function updateTaskRuntime(taskId: string, runToken: string, patch: Partial<TaskRecord>) {
    const current = store.get(taskId);
    if (!current || current.runToken !== runToken || current.status === "stopped") {
      return undefined;
    }

    return store.update(taskId, patch);
  }
}

function pipeProcessLogs(child: ChildProcess, logStream: fs.WriteStream) {
  child.stdout?.pipe(logStream, { end: false });
  child.stderr?.pipe(logStream, { end: false });
  child.once("close", () => {
    logStream.end();
  });
}

function prepareEventLogDirectory(task: TaskRecord, eventsDir: string) {
  if (!task.sourcePath) {
    throw new Error(`Task ${task.id} is missing a source eventlog path.`);
  }

  fs.mkdirSync(eventsDir, { recursive: true });
  const linkedEventLog = path.join(eventsDir, path.basename(task.sourcePath));
  fs.rmSync(linkedEventLog, { force: true });
  try {
    fs.symlinkSync(task.sourcePath, linkedEventLog);
  } catch {
    fs.copyFileSync(task.sourcePath, linkedEventLog);
  }
}

function waitForExit(child: ChildProcess, commandName: string): Promise<number> {
  return new Promise((resolve, reject) => {
    child.once("error", (error) => {
      reject(new Error(`${commandName} failed to start: ${error.message}`));
    });
    child.once("close", (code) => {
      resolve(code ?? 1);
    });
  });
}

function buildEventLogPreprocessorArgs(
  runtime: SparkRuntime,
  input: EventLogPreprocessorLaunchInput
): string[] {
  return [
    "--class",
    "org.apache.spark.deploy.history.EventLogPreprocessor",
    runtime.projectJar,
    "--eventlog",
    input.eventLogPath,
    "--uimeta-dir",
    input.uimetaDir,
    "--result-file",
    input.resultFile
  ];
}

function reserveHistoryPort(startPort: number): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.once("error", reject);
    server.listen(startPort, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close(() => reject(new Error("Failed to determine a free history server port.")));
        return;
      }

      const port = address.port;
      server.close((error) => {
        if (error) {
          reject(error);
          return;
        }

        resolve(port);
      });
    });
  });
}

function waitForPortReady(port: number, timeoutMs: number): Promise<void> {
  const startedAt = Date.now();

  return new Promise((resolve, reject) => {
    const tryConnect = () => {
      const socket = net.createConnection({ host: "127.0.0.1", port });
      socket.once("connect", () => {
        socket.end();
        resolve();
      });
      socket.once("error", () => {
        socket.destroy();
        if (Date.now() - startedAt >= timeoutMs) {
          reject(new Error(`HistoryServer did not become reachable on port ${port} within ${timeoutMs}ms.`));
          return;
        }

        setTimeout(tryConnect, 250);
      });
    };

    tryConnect();
  });
}

export { resolveSparkRuntime } from "./spark-runtime";
