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
}
