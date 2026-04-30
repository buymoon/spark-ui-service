export type TaskStatus =
  | "queued"
  | "validating"
  | "preprocessing"
  | "starting_history_server"
  | "ready"
  | "failed"
  | "stopped";

export type TaskInputMode = "upload" | "path";

export interface TaskSummary {
  id: string;
  status: TaskStatus;
  inputMode: TaskInputMode;
  sourceLabel: string;
  createdAt: string;
  appId?: string;
  historyUrl?: string;
  outputMode?: "managed" | "custom";
}

export interface TaskDetail extends TaskSummary {
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
  historyServerState?: "preprocessing" | "starting" | "ready" | "failed" | "stopped";
  errorSummary?: string;
}

export interface TaskLogs {
  preprocessor: string;
  historyServer: string;
}

export interface CreatePathTaskInput {
  inputMode: "path";
  eventLogPath: string;
  outputMode: "managed" | "custom";
  outputDir?: string;
}

export interface CreateUploadTaskInput {
  inputMode: "upload";
  eventLogFile: File;
  outputMode: "managed" | "custom";
  outputDir?: string;
}
