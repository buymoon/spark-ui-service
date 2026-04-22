import type { TaskLogs } from "../types";

interface LogsPanelProps {
  logs: TaskLogs | null;
}

export function LogsPanel({ logs }: LogsPanelProps) {
  if (!logs) {
    return <p className="muted-copy">Logs will appear after the selected task has emitted output.</p>;
  }

  return (
    <div className="logs-grid">
      <section>
        <h4>Preprocessor Log</h4>
        <pre>{logs.preprocessor || "No preprocessor log yet."}</pre>
      </section>
      <section>
        <h4>History Server Log</h4>
        <pre>{logs.historyServer || "No history server log yet."}</pre>
      </section>
    </div>
  );
}
