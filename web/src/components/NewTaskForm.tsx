import { useState } from "react";
import { createTask } from "../api";
import type { TaskSummary } from "../types";

interface NewTaskFormProps {
  onCreated(task: TaskSummary): void;
}

export function NewTaskForm({ onCreated }: NewTaskFormProps) {
  const [mode, setMode] = useState<"upload" | "path">("upload");
  const [eventLogPath, setEventLogPath] = useState("");
  const [outputMode, setOutputMode] = useState<"managed" | "custom">("managed");
  const [outputDir, setOutputDir] = useState("");
  const [eventLogFile, setEventLogFile] = useState<File | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitting(true);
    setError(null);

    try {
      const task =
        mode === "upload"
          ? await createTask({
              inputMode: "upload",
              eventLogFile: eventLogFile ?? new File([], "empty.eventlog"),
              outputMode,
              outputDir: outputMode === "custom" ? outputDir : undefined
            })
          : await createTask({
              inputMode: "path",
              eventLogPath,
              outputMode,
              outputDir: outputMode === "custom" ? outputDir : undefined
            });
      onCreated(task);
    } catch (submitError) {
      setError(submitError instanceof Error ? submitError.message : String(submitError));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <form className="panel-form" onSubmit={handleSubmit}>
      <div className="tab-row" role="tablist" aria-label="Task input mode">
        <button
          type="button"
          className={`tab ${mode === "upload" ? "active" : ""}`}
          aria-pressed={mode === "upload"}
          onClick={() => setMode("upload")}
        >
          Upload File
        </button>
        <button
          type="button"
          className={`tab ${mode === "path" ? "active" : ""}`}
          aria-pressed={mode === "path"}
          onClick={() => setMode("path")}
        >
          Local Path
        </button>
      </div>

      <p className="hint">Recommended for very large eventlogs: use Local Path mode.</p>

      {mode === "upload" ? (
        <label className="field" key="upload-mode">
          <span>Eventlog File</span>
          <input
            type="file"
            accept=".eventlog,.json,.lz4,.gz"
            onChange={(event) => setEventLogFile(event.target.files?.[0] ?? null)}
          />
        </label>
      ) : (
        <label className="field" key="path-mode">
          <span>Absolute Eventlog Path</span>
          <input
            aria-label="Absolute eventlog path"
            type="text"
            placeholder="/tmp/spark-events/application_123.eventlog"
            value={eventLogPath}
            onChange={(event) => setEventLogPath(event.target.value)}
          />
        </label>
      )}

      <div className="field-row">
        <label className="field">
          <span>Output Mode</span>
          <select
            value={outputMode}
            onChange={(event) => setOutputMode(event.target.value as "managed" | "custom")}
          >
            <option value="managed">Managed Workspace</option>
            <option value="custom">Custom Directory</option>
          </select>
        </label>

        {outputMode === "custom" ? (
          <label className="field">
            <span>Custom Output Directory</span>
            <input
              type="text"
              placeholder="/absolute/output/path"
              value={outputDir}
              onChange={(event) => setOutputDir(event.target.value)}
            />
          </label>
        ) : null}
      </div>

      {error ? <p className="inline-error">{error}</p> : null}

      <button className="primary-action" type="submit" disabled={submitting}>
        {submitting ? "Working..." : "Generate And Launch History UI"}
      </button>
    </form>
  );
}
