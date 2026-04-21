import type { TaskInputMode, TaskStatus, TaskSummary } from "../src/types";

export interface TaskRecord extends TaskSummary {
  outputMode: "managed" | "custom";
  workspaceDir: string;
  sourcePath?: string;
  uploadedFileName?: string;
  outputDir?: string;
  updatedAt: string;
  errorSummary?: string;
}

export interface AppContext {
  workspaceRoot: string;
}

export interface TaskResponse extends TaskSummary {
  outputMode: "managed" | "custom";
}

export type { TaskInputMode, TaskStatus };
