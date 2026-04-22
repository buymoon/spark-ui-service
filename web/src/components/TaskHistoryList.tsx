import type { TaskSummary } from "../types";

interface TaskHistoryListProps {
  tasks: TaskSummary[];
  selectedTaskId: string | null;
  onSelect(taskId: string): void;
}

export function TaskHistoryList({ tasks, selectedTaskId, onSelect }: TaskHistoryListProps) {
  if (tasks.length === 0) {
    return <p className="muted-copy">No tasks yet. Create one from the panel on the left.</p>;
  }

  return (
    <div className="history-list">
      {tasks.map((task) => (
        <button
          key={task.id}
          type="button"
          className={`history-card ${task.id === selectedTaskId ? "selected" : ""}`}
          onClick={() => onSelect(task.id)}
        >
          <span className={`status-pill status-${task.status}`}>{task.status}</span>
          <strong>{task.sourceLabel}</strong>
          <span>{task.inputMode === "path" ? "Local Path" : "Upload"}</span>
          <span>{task.createdAt}</span>
        </button>
      ))}
    </div>
  );
}
