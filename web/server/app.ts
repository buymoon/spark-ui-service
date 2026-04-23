import express from "express";
import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import multer from "multer";
import path from "node:path";
import { TaskStore } from "./task-store";
import type {
  AppContext,
  TaskDetailResponse,
  TaskExecutor,
  TaskInputMode,
  TaskExecutionResult,
  TaskRecord,
  TaskResponse
} from "./types";

function isPathTaskRequest(body: unknown): body is {
  inputMode: TaskInputMode;
  eventLogPath: string;
  outputMode?: "managed" | "custom";
  outputDir?: string;
} {
  if (typeof body !== "object" || body === null) {
    return false;
  }

  const record = body as Record<string, unknown>;
  return record.inputMode === "path" && typeof record.eventLogPath === "string";
}

export function createApp(context: AppContext) {
  const app = express();
  const store = new TaskStore(context.workspaceRoot);
  const taskExecutor = context.taskExecutor ?? createNoopTaskExecutor();
  const uploadsDir = path.join(context.workspaceRoot, ".uploads");
  fs.mkdirSync(uploadsDir, { recursive: true });
  const upload = multer({ dest: uploadsDir });

  app.use(express.json());

  app.post("/api/tasks", upload.single("eventLogFile"), (req, res) => {
    const stagedUploadPath = req.file?.path;

    if (req.body?.inputMode === "upload") {
      if (!req.file) {
        return res.status(400).json({ error: "eventLogFile is required for upload mode." });
      }

      const outputMode = normalizeOutputMode(req.body.outputMode);
      if (req.body.outputMode !== undefined && outputMode === undefined) {
        cleanupUploadedFile(req.file.path);
        return res.status(400).json({ error: "outputMode must be either \"managed\" or \"custom\"." });
      }
      const outputDir = normalizeOutputDir(req.body.outputDir);
      if (outputMode === "custom" && !outputDir) {
        cleanupUploadedFile(req.file.path);
        return res.status(400).json({ error: "A custom outputDir is required for custom output mode." });
      }

      const task = store.create({
        status: "queued",
        inputMode: "upload",
        outputMode: outputMode ?? "managed",
        outputDir,
        workspaceDir: outputMode === "custom" ? outputDir : undefined,
        sourceLabel: req.file.originalname,
        uploadedFileName: req.file.originalname,
        sourcePath: req.file.path,
        historyUrl: undefined,
        appId: undefined
      });

      enqueueTask(taskExecutor, store, task.id, task.runToken);
      return res.status(201).json({ task: toTaskResponse(task) });
    }

    if (!isPathTaskRequest(req.body)) {
      cleanupUploadedFile(stagedUploadPath);
      return res.status(400).json({ error: "An absolute eventlog path is required." });
    }

    const { eventLogPath } = req.body;
    const outputMode = normalizeOutputMode(req.body.outputMode);
    if (req.body.outputMode !== undefined && outputMode === undefined) {
      cleanupUploadedFile(stagedUploadPath);
      return res.status(400).json({ error: "outputMode must be either \"managed\" or \"custom\"." });
    }
    const outputDir = normalizeOutputDir(req.body.outputDir);
    if (outputMode === "custom" && !outputDir) {
      cleanupUploadedFile(stagedUploadPath);
      return res.status(400).json({ error: "A custom outputDir is required for custom output mode." });
    }

    if (!path.isAbsolute(eventLogPath) || !isReadableFile(eventLogPath)) {
      cleanupUploadedFile(stagedUploadPath);
      return res.status(400).json({ error: "An absolute eventlog path is required." });
    }

    const task = store.create({
      status: "queued",
      inputMode: "path",
      outputMode: outputMode ?? "managed",
      outputDir,
      workspaceDir: outputMode === "custom" ? outputDir : undefined,
      sourceLabel: path.basename(eventLogPath),
      sourcePath: eventLogPath,
      historyUrl: undefined,
      appId: undefined
    });

    cleanupUploadedFile(stagedUploadPath);
    enqueueTask(taskExecutor, store, task.id, task.runToken);
    return res.status(201).json({ task: toTaskResponse(task) });
  });

  app.get("/api/tasks", (_req, res) => {
    return res.json({ tasks: store.list().map(toTaskResponse) });
  });

  app.get("/api/tasks/:id", (req, res) => {
    const task = store.get(req.params.id);
    if (!task) {
      return res.status(404).json({ error: "Task not found." });
    }

    return res.json({ task: toTaskDetailResponse(task) });
  });

  app.get("/api/tasks/:id/logs", (req, res) => {
    const task = store.get(req.params.id);
    if (!task) {
      return res.status(404).json({ error: "Task not found." });
    }

    return res.json({
      logs: {
        preprocessor: readTaskLog(task.preprocessorLogPath),
        historyServer: readTaskLog(task.historyServerLogPath)
      }
    });
  });

  app.get("/history-proxy/:id/*", (req, res) => {
    const task = store.get(req.params.id);
    if (!task) {
      return res.status(404).send("Task not found.");
    }
    if (!task.historyPort) {
      return res.status(409).send("History server is not ready for this task.");
    }

    const queryIndex = req.originalUrl.indexOf("?");
    const querySuffix = queryIndex >= 0 ? req.originalUrl.slice(queryIndex) : "";
    const upstreamPath = `/${req.params[0] ?? ""}${querySuffix}`;
    const upstream = http.request(
      {
        host: "127.0.0.1",
        port: task.historyPort,
        path: upstreamPath,
        method: "GET",
        headers: {
          ...req.headers,
          host: `127.0.0.1:${task.historyPort}`,
          "accept-encoding": "identity"
        }
      },
      (upstreamResponse) => {
        const statusCode = upstreamResponse.statusCode ?? 502;
        const contentType = String(upstreamResponse.headers["content-type"] ?? "");
        const rewriteHtml = contentType.startsWith("text/html");

        if (!rewriteHtml) {
          copyProxyHeaders(upstreamResponse.headers, res, req.params.id);
          res.status(statusCode);
          upstreamResponse.pipe(res);
          return;
        }

        const chunks: Buffer[] = [];
        upstreamResponse.on("data", (chunk) => {
          chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
        });
        upstreamResponse.on("end", () => {
          const body = Buffer.concat(chunks).toString("utf8");
          copyProxyHeaders(upstreamResponse.headers, res, req.params.id, { rewriteBody: true });
          res.status(statusCode).send(rewriteSparkHistoryHtml(body, req.params.id));
        });
      }
    );

    upstream.on("error", (error) => {
      if (!res.headersSent) {
        res.status(502).send(`Failed to reach the history server: ${error.message}`);
      }
    });
    upstream.end();
  });

  app.post("/api/tasks/:id/stop", async (req, res) => {
    const task = store.get(req.params.id);
    if (!task) {
      return res.status(404).json({ error: "Task not found." });
    }

    const stoppedTask = store.update(task.id, {
      status: "stopped",
      runToken: crypto.randomUUID()
    });
    await taskExecutor.stop(task.id);
    return res.json({ task: toTaskResponse(stoppedTask) });
  });

  app.post("/api/tasks/:id/retry", async (req, res) => {
    const task = store.get(req.params.id);
    if (!task) {
      return res.status(404).json({ error: "Task not found." });
    }
    if (task.status !== "failed" && task.status !== "stopped") {
      return res.status(409).json({
        error: "Only failed or stopped tasks can be retried.",
        task: toTaskResponse(task)
      });
    }

    await taskExecutor.stop(task.id);
    const nextTask = store.update(task.id, {
      status: "queued",
      runToken: crypto.randomUUID(),
      appId: undefined,
      historyUrl: undefined,
      errorSummary: undefined
    });
    try {
      const result = await taskExecutor.enqueue(task.id);
      const updated = updateTaskIfActive(store, task.id, nextTask.runToken, result);
      return res.json({ task: toTaskResponse(updated ?? store.get(task.id) ?? nextTask) });
    } catch (error) {
      const failedTask = updateTaskIfActive(store, task.id, nextTask.runToken, {
        status: "failed",
        errorSummary: error instanceof Error ? error.message : String(error)
      });
      return res.status(500).json({
        error: "Task retry failed.",
        task: toTaskResponse(failedTask ?? store.get(task.id) ?? nextTask)
      });
    }
  });

  app.post("/api/tasks/:id/restart", async (req, res) => {
    const task = store.get(req.params.id);
    if (!task) {
      return res.status(404).json({ error: "Task not found." });
    }
    if (task.status !== "ready") {
      return res.status(409).json({
        error: "Only ready tasks can restart their history service.",
        task: toTaskResponse(task)
      });
    }

    await taskExecutor.stop(task.id);
    const nextTask = store.update(task.id, {
      status: "queued",
      runToken: crypto.randomUUID(),
      errorSummary: undefined
    });
    try {
      const result = await taskExecutor.enqueue(task.id);
      const updated = updateTaskIfActive(store, task.id, nextTask.runToken, result);
      return res.json({ task: toTaskResponse(updated ?? store.get(task.id) ?? nextTask) });
    } catch (error) {
      const failedTask = updateTaskIfActive(store, task.id, nextTask.runToken, {
        status: "failed",
        errorSummary: error instanceof Error ? error.message : String(error)
      });
      return res.status(500).json({
        error: "Task restart failed.",
        task: toTaskResponse(failedTask ?? store.get(task.id) ?? nextTask)
      });
    }
  });

  return app;
}

function enqueueTask(taskExecutor: TaskExecutor, store: TaskStore, taskId: string, runToken: string) {
  void taskExecutor
    .enqueue(taskId)
    .then((result) => updateTaskIfActive(store, taskId, runToken, result))
    .catch((error: unknown) => {
      updateTaskIfActive(store, taskId, runToken, {
        status: "failed",
        errorSummary: error instanceof Error ? error.message : String(error)
      });
    });
}

function updateTaskIfActive(
  store: TaskStore,
  taskId: string,
  runToken: string,
  patch: TaskExecutionResult
) {
  const current = store.get(taskId);
  if (!current || current.status === "stopped" || current.runToken !== runToken) {
    return undefined;
  }

  return store.update(taskId, patch);
}

function createNoopTaskExecutor(): TaskExecutor {
  return {
    enqueue: async () => ({ status: "queued" }),
    stop: async () => undefined
  };
}

function isReadableFile(filePath: string) {
  try {
    const stat = fs.statSync(filePath);
    fs.accessSync(filePath, fs.constants.R_OK);
    return stat.isFile();
  } catch {
    return false;
  }
}

function normalizeOutputMode(outputMode: unknown): "managed" | "custom" | undefined {
  if (outputMode === undefined) {
    return undefined;
  }

  if (outputMode === "managed" || outputMode === "custom") {
    return outputMode;
  }

  return undefined;
}

function normalizeOutputDir(outputDir: unknown): string | undefined {
  if (typeof outputDir !== "string" || !path.isAbsolute(outputDir)) {
    return undefined;
  }

  return outputDir;
}

function toTaskResponse(task: TaskRecord): TaskResponse {
  return {
    id: task.id,
    status: task.status,
    inputMode: task.inputMode,
    sourceLabel: task.sourceLabel,
    createdAt: task.createdAt,
    appId: task.appId,
    historyUrl: resolveTaskHistoryUrl(task),
    outputMode: task.outputMode
  };
}

function toTaskDetailResponse(task: TaskRecord): TaskDetailResponse {
  return {
    ...toTaskResponse(task),
    updatedAt: task.updatedAt,
    uploadedFileName: task.uploadedFileName,
    attemptId: task.attemptId,
    uimetaPath: task.uimetaPath,
    historyPort: task.historyPort,
    executionStartedAt: task.executionStartedAt,
    preprocessorPid: task.preprocessorPid,
    historyServerPid: task.historyServerPid,
    preprocessorLogPath: task.preprocessorLogPath,
    historyServerLogPath: task.historyServerLogPath,
    preprocessorCommand: task.preprocessorCommand,
    historyServerCommand: task.historyServerCommand,
    historyServerState: task.historyServerState,
    errorSummary: task.errorSummary
  };
}

function cleanupUploadedFile(filePath: string) {
  try {
    fs.rmSync(filePath, { force: true });
  } catch {
    // Best-effort cleanup only.
  }
}

function readTaskLog(filePath?: string) {
  try {
    if (!filePath || !fs.existsSync(filePath)) {
      return "";
    }

    return fs.readFileSync(filePath, "utf8");
  } catch {
    return "";
  }
}

function resolveTaskHistoryUrl(task: TaskRecord) {
  if (task.appId && task.historyPort) {
    return `/history-proxy/${task.id}/history/${task.appId}/jobs/`;
  }

  return task.historyUrl;
}

function copyProxyHeaders(
  headers: http.IncomingHttpHeaders,
  response: express.Response,
  taskId: string,
  options?: { rewriteBody?: boolean }
) {
  for (const [headerName, headerValue] of Object.entries(headers)) {
    if (!headerValue) {
      continue;
    }

    const normalizedName = headerName.toLowerCase();
    if (
      normalizedName === "connection" ||
      normalizedName === "keep-alive" ||
      normalizedName === "transfer-encoding" ||
      normalizedName === "content-encoding" ||
      (options?.rewriteBody && normalizedName === "content-length")
    ) {
      continue;
    }

    if (normalizedName === "location") {
      response.setHeader(headerName, rewriteProxyLocationHeader(headerValue, taskId));
      continue;
    }

    response.setHeader(headerName, headerValue);
  }
}

function rewriteProxyLocationHeader(headerValue: string | string[], taskId: string) {
  if (Array.isArray(headerValue)) {
    return headerValue.map((value) => rewriteProxyLocationHeader(value, taskId));
  }

  if (headerValue.startsWith("/static/")) {
    return `/history-proxy/${taskId}${headerValue}`;
  }
  if (headerValue.startsWith("/history/")) {
    return `/history-proxy/${taskId}${headerValue}`;
  }

  return headerValue;
}

function rewriteSparkHistoryHtml(html: string, taskId: string) {
  const proxyRoot = `/history-proxy/${taskId}`;
  return html
    .replaceAll('href="/static/', `href="${proxyRoot}/static/`)
    .replaceAll('src="/static/', `src="${proxyRoot}/static/`)
    .replaceAll("setUIRoot('')", `setUIRoot('${proxyRoot}')`)
    .replaceAll("setAppBasePath('/history/", `setAppBasePath('${proxyRoot}/history/`)
    .replaceAll('href="/history/', `href="${proxyRoot}/history/`)
    .replaceAll('src="/history/', `src="${proxyRoot}/history/`);
}
