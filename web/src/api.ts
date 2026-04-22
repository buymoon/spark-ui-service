import type {
  CreatePathTaskInput,
  CreateUploadTaskInput,
  TaskDetail,
  TaskLogs,
  TaskSummary
} from "./types";

async function readJson<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const body = await response.text();
    throw new Error(body || `Request failed with status ${response.status}`);
  }

  return response.json() as Promise<T>;
}

export async function fetchTasks(): Promise<TaskSummary[]> {
  const response = await fetch("/api/tasks");
  const body = await readJson<{ tasks: TaskSummary[] }>(response);
  return body.tasks;
}

export async function fetchTask(taskId: string): Promise<TaskDetail> {
  const response = await fetch(`/api/tasks/${taskId}`);
  const body = await readJson<{ task: TaskDetail }>(response);
  return body.task;
}

export async function fetchLogs(taskId: string): Promise<TaskLogs> {
  const response = await fetch(`/api/tasks/${taskId}/logs`);
  const body = await readJson<{ logs: TaskLogs }>(response);
  return body.logs;
}

export async function createTask(
  input: CreatePathTaskInput | CreateUploadTaskInput
): Promise<TaskSummary> {
  if (input.inputMode === "upload") {
    const formData = new FormData();
    formData.append("inputMode", "upload");
    formData.append("outputMode", input.outputMode);
    if (input.outputDir) {
      formData.append("outputDir", input.outputDir);
    }
    formData.append("eventLogFile", input.eventLogFile);

    const response = await fetch("/api/tasks", {
      method: "POST",
      body: formData
    });
    const body = await readJson<{ task: TaskSummary }>(response);
    return body.task;
  }

  const response = await fetch("/api/tasks", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input)
  });
  const body = await readJson<{ task: TaskSummary }>(response);
  return body.task;
}

export async function stopTask(taskId: string): Promise<TaskSummary> {
  const response = await fetch(`/api/tasks/${taskId}/stop`, { method: "POST" });
  const body = await readJson<{ task: TaskSummary }>(response);
  return body.task;
}

export async function retryTask(taskId: string): Promise<TaskSummary> {
  const response = await fetch(`/api/tasks/${taskId}/retry`, { method: "POST" });
  const body = await readJson<{ task: TaskSummary }>(response);
  return body.task;
}

export async function restartTask(taskId: string): Promise<TaskSummary> {
  const response = await fetch(`/api/tasks/${taskId}/restart`, { method: "POST" });
  const body = await readJson<{ task: TaskSummary }>(response);
  return body.task;
}
