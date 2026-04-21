import express from "express";
import fs from "node:fs";
import multer from "multer";
import path from "node:path";
import { TaskStore } from "./task-store";
import type {
  AppContext,
  TaskDetailResponse,
  TaskInputMode,
  TaskRecord,
  TaskResponse
} from "./types";

function isPathTaskRequest(body: unknown): body is {
  inputMode: TaskInputMode;
  eventLogPath: string;
  outputMode?: "managed" | "custom";
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

      const task = store.create({
        status: "queued",
        inputMode: "upload",
        outputMode: outputMode ?? "managed",
        sourceLabel: req.file.originalname,
        uploadedFileName: req.file.originalname,
        sourcePath: req.file.path,
        historyUrl: undefined,
        appId: undefined
      });

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

    if (!path.isAbsolute(eventLogPath) || !isReadableFile(eventLogPath)) {
      cleanupUploadedFile(stagedUploadPath);
      return res.status(400).json({ error: "An absolute eventlog path is required." });
    }

    const task = store.create({
      status: "queued",
      inputMode: "path",
      outputMode: outputMode ?? "managed",
      sourceLabel: path.basename(eventLogPath),
      sourcePath: eventLogPath,
      historyUrl: undefined,
      appId: undefined
    });

    cleanupUploadedFile(stagedUploadPath);

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
        preprocessor: "",
        historyServer: ""
      }
    });
  });

  return app;
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

function toTaskResponse(task: TaskRecord): TaskResponse {
  return {
    id: task.id,
    status: task.status,
    inputMode: task.inputMode,
    sourceLabel: task.sourceLabel,
    createdAt: task.createdAt,
    appId: task.appId,
    historyUrl: task.historyUrl,
    outputMode: task.outputMode
  };
}

function toTaskDetailResponse(task: TaskRecord): TaskDetailResponse {
  return {
    ...toTaskResponse(task),
    uploadedFileName: task.uploadedFileName
  };
}

function cleanupUploadedFile(filePath: string) {
  try {
    fs.rmSync(filePath, { force: true });
  } catch {
    // Best-effort cleanup only.
  }
}
