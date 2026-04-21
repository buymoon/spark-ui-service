import express from "express";
import fs from "node:fs";
import path from "node:path";
import { TaskStore } from "./task-store";
import type { AppContext, TaskInputMode, TaskRecord, TaskResponse } from "./types";

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

  app.use(express.json());

  app.post("/api/tasks", (req, res) => {
    if (!isPathTaskRequest(req.body)) {
      return res.status(400).json({ error: "An absolute eventlog path is required." });
    }

    const { eventLogPath, outputMode = "managed" } = req.body;
    if (outputMode !== "managed" && outputMode !== "custom") {
      return res.status(400).json({ error: "outputMode must be either \"managed\" or \"custom\"." });
    }

    if (!path.isAbsolute(eventLogPath) || !isReadableFile(eventLogPath)) {
      return res.status(400).json({ error: "An absolute eventlog path is required." });
    }

    const task = store.create({
      status: "queued",
      inputMode: "path",
      outputMode,
      sourceLabel: path.basename(eventLogPath),
      sourcePath: eventLogPath,
      historyUrl: undefined,
      appId: undefined
    });

    return res.status(201).json({ task: toTaskResponse(task) });
  });

  app.get("/api/tasks", (_req, res) => {
    return res.json({ tasks: store.list().map(toTaskResponse) });
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
