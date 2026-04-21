import crypto from "node:crypto";
import fs from "node:fs";
import { taskFile, taskWorkspace } from "./paths";
import type { TaskInputMode, TaskRecord, TaskStatus } from "./types";

const SUPPORTED_TASK_STATUSES = new Set<TaskStatus>([
  "queued",
  "validating",
  "preprocessing",
  "starting_history_server",
  "ready",
  "failed",
  "stopped"
]);

const SUPPORTED_TASK_INPUT_MODES = new Set<TaskInputMode>(["upload", "path"]);

const SUPPORTED_TASK_OUTPUT_MODES = new Set<TaskRecord["outputMode"]>(["managed", "custom"]);

export class TaskStore {
  constructor(private readonly workspaceRoot: string) {
    fs.mkdirSync(this.workspaceRoot, { recursive: true });
  }

  create(record: Omit<TaskRecord, "id" | "createdAt" | "updatedAt" | "workspaceDir">): TaskRecord {
    const id = crypto.randomUUID();
    const now = new Date().toISOString();
    const task: TaskRecord = {
      ...record,
      id,
      workspaceDir: taskWorkspace(this.workspaceRoot, id),
      createdAt: now,
      updatedAt: now
    };

    fs.mkdirSync(task.workspaceDir, { recursive: true });
    fs.writeFileSync(taskFile(this.workspaceRoot, id, "task.json"), JSON.stringify(task, null, 2));
    return task;
  }

  list(): TaskRecord[] {
    return fs
      .readdirSync(this.workspaceRoot, { withFileTypes: true })
      .filter((entry) => entry.isDirectory())
      .map((entry) => this.readTask(entry.name))
      .filter((task): task is TaskRecord => task !== undefined)
      .sort((a, b) => b.createdAt.localeCompare(a.createdAt));
  }

  get(taskId: string): TaskRecord | undefined {
    return this.readTask(taskId);
  }

  private readTask(taskId: string): TaskRecord | undefined {
    const taskPath = taskFile(this.workspaceRoot, taskId, "task.json");
    if (!fs.existsSync(taskPath)) {
      return undefined;
    }

    try {
      const parsed = JSON.parse(fs.readFileSync(taskPath, "utf8")) as Partial<TaskRecord>;
      if (!this.isTaskRecord(parsed)) {
        this.warnSkippedTask(taskId, "task.json is missing required fields or contains unsupported values");
        return undefined;
      }

      return parsed;
    } catch {
      this.warnSkippedTask(taskId, "task.json could not be parsed");
      return undefined;
    }
  }

  private isTaskRecord(task: Partial<TaskRecord> | undefined): task is TaskRecord {
    return Boolean(
      task &&
        typeof task.id === "string" &&
        typeof task.status === "string" &&
        SUPPORTED_TASK_STATUSES.has(task.status as TaskStatus) &&
        typeof task.inputMode === "string" &&
        SUPPORTED_TASK_INPUT_MODES.has(task.inputMode as TaskInputMode) &&
        typeof task.sourceLabel === "string" &&
        typeof task.createdAt === "string" &&
        typeof task.updatedAt === "string" &&
        typeof task.outputMode === "string" &&
        SUPPORTED_TASK_OUTPUT_MODES.has(task.outputMode as TaskRecord["outputMode"]) &&
        typeof task.workspaceDir === "string"
    );
  }

  private warnSkippedTask(taskId: string, reason: string) {
    console.warn(`[task-store] skipped task ${taskId}: ${reason}`);
  }
}
