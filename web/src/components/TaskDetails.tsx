import { LogsPanel } from "./LogsPanel";
import type { TaskDetail, TaskLogs } from "../types";

interface TaskDetailsProps {
  task: TaskDetail | null;
  logs: TaskLogs | null;
  onStop(): void;
  onRetry(): void;
  onRestart(): void;
}

export function TaskDetails({ task, logs, onStop, onRetry, onRestart }: TaskDetailsProps) {
  if (!task) {
    return <p className="muted-copy">Select a task to inspect its status, metadata, and logs.</p>;
  }

  return (
    <div className="details-stack">
      <div className="status-strip">
        <span className={`status-pill status-${task.status}`}>{task.status}</span>
        <span>{task.sourceLabel}</span>
      </div>

      <dl className="details-grid">
        <div>
          <dt>App ID</dt>
          <dd>{task.appId ?? "Pending"}</dd>
        </div>
        <div>
          <dt>Attempt</dt>
          <dd>{task.attemptId ?? "n/a"}</dd>
        </div>
        <div>
          <dt>History URL</dt>
          <dd>{task.historyUrl ?? "Not ready"}</dd>
        </div>
        <div>
          <dt>UIMeta</dt>
          <dd>{task.uimetaPath ?? "Not generated yet"}</dd>
        </div>
        <div>
          <dt>History Port</dt>
          <dd>{task.historyPort ?? "Pending"}</dd>
        </div>
        <div>
          <dt>Server State</dt>
          <dd>{task.historyServerState ?? "Unknown"}</dd>
        </div>
        <div>
          <dt>Updated</dt>
          <dd>{task.updatedAt}</dd>
        </div>
        <div>
          <dt>Error</dt>
          <dd>{task.errorSummary ?? "None"}</dd>
        </div>
      </dl>

      <div className="action-row">
        <button type="button" onClick={onStop}>
          Stop Service
        </button>
        <button type="button" onClick={onRetry}>
          Retry Task
        </button>
        <button type="button" onClick={onRestart}>
          Restart Service
        </button>
        {task.historyUrl ? (
          <a className="primary-action link-action" href={task.historyUrl} target="_blank" rel="noreferrer">
            Open Spark UI
          </a>
        ) : null}
      </div>

      <LogsPanel logs={logs} />
    </div>
  );
}
