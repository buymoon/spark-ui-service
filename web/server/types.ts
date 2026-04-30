import type { TaskInputMode, TaskStatus, TaskSummary } from "../src/types";

export interface TaskRecord extends TaskSummary {
  outputMode: "managed" | "custom";
  runToken: string;
  attemptId?: string;
  uimetaPath?: string;
  historyPort?: number;
  executionStartedAt?: string;
  preprocessorPid?: number;
  historyServerPid?: number;
  preprocessorLogPath?: string;
  historyServerLogPath?: string;
  preprocessorCommand?: string[];
  historyServerCommand?: string[];
  historyServerState?: "preprocessing" | "starting" | "ready" | "failed" | "stopped";
  workspaceDir: string;
  sourcePath?: string;
  uploadedFileName?: string;
  outputDir?: string;
  updatedAt: string;
  errorSummary?: string;
}

export interface AppContext {
  workspaceRoot: string;
  taskExecutor?: TaskExecutor;
}

export interface TaskResponse extends TaskSummary {
  outputMode: "managed" | "custom";
}

export interface TaskDetailResponse extends TaskResponse {
  updatedAt: string;
  uploadedFileName?: string;
  attemptId?: string;
  uimetaPath?: string;
  historyPort?: number;
  executionStartedAt?: string;
  preprocessorPid?: number;
  historyServerPid?: number;
  preprocessorLogPath?: string;
  historyServerLogPath?: string;
  preprocessorCommand?: string[];
  historyServerCommand?: string[];
  historyServerState?: TaskRecord["historyServerState"];
  errorSummary?: string;
}

export interface TaskExecutionResult {
  status: TaskStatus;
  appId?: string;
  attemptId?: string;
  uimetaPath?: string;
  historyUrl?: string;
  historyPort?: number;
  errorSummary?: string;
}

export interface TaskExecutor {
  enqueue(taskId: string): Promise<TaskExecutionResult>;
  stop(taskId: string): Promise<void>;
}

export type { TaskInputMode, TaskStatus };
