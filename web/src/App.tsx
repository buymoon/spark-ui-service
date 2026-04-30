import { useEffect, useState } from "react";
import { fetchLogs, fetchTask, fetchTasks, restartTask, retryTask, stopTask } from "./api";
import { NewTaskForm } from "./components/NewTaskForm";
import { TaskDetails } from "./components/TaskDetails";
import { TaskHistoryList } from "./components/TaskHistoryList";
import type { TaskDetail, TaskLogs, TaskSummary } from "./types";
import "./styles.css";

export default function App() {
  const [tasks, setTasks] = useState<TaskSummary[]>([]);
  const [selectedTaskId, setSelectedTaskId] = useState<string | null>(null);
  const [selectedTask, setSelectedTask] = useState<TaskDetail | null>(null);
  const [logs, setLogs] = useState<TaskLogs | null>(null);

  async function refreshTasks() {
    const nextTasks = await fetchTasks();
    setTasks(nextTasks);
    if (!selectedTaskId && nextTasks.length > 0) {
      setSelectedTaskId(nextTasks[0].id);
    }
  }

  async function refreshSelectedTask(taskId: string) {
    const [task, taskLogs] = await Promise.all([fetchTask(taskId), fetchLogs(taskId)]);
    setSelectedTask(task);
    setLogs(taskLogs);
  }

  useEffect(() => {
    void refreshTasks();
  }, []);

  useEffect(() => {
    if (!selectedTaskId) {
      setSelectedTask(null);
      setLogs(null);
      return;
    }

    void refreshSelectedTask(selectedTaskId);
  }, [selectedTaskId]);

  useEffect(() => {
    if (!selectedTaskId || !selectedTask) {
      return;
    }

    if (!["queued", "preprocessing", "starting_history_server", "validating"].includes(selectedTask.status)) {
      return;
    }

    const timer = window.setInterval(() => {
      void refreshTasks();
      void refreshSelectedTask(selectedTaskId);
    }, 3000);

    return () => window.clearInterval(timer);
  }, [selectedTask, selectedTaskId]);

  async function handleRefreshAfterAction(task: TaskSummary) {
    await refreshTasks();
    setSelectedTaskId(task.id);
    await refreshSelectedTask(task.id);
  }

  return (
    <main className="app-shell">
      <header className="hero">
        <p className="eyebrow">Single-Machine Workflow</p>
        <h1>Spark History UI Loader</h1>
        <p className="subtitle">
          Turn a local Spark eventlog into a reusable `.uimeta`, boot a task-scoped History Server,
          and open the native Spark UI without waiting on full replay every time.
        </p>
      </header>

      <section className="workspace-grid">
        <section className="panel" aria-labelledby="new-task-title">
          <h2 id="new-task-title">New Task</h2>
          <NewTaskForm
            onCreated={(task) => {
              setSelectedTaskId(task.id);
              void refreshTasks();
            }}
          />
        </section>

        <section className="panel" aria-labelledby="current-task-title">
          <h2 id="current-task-title">Current Task</h2>
          <TaskDetails
            task={selectedTask}
            logs={logs}
            onStop={() => {
              if (selectedTaskId) {
                void stopTask(selectedTaskId).then(handleRefreshAfterAction);
              }
            }}
            onRetry={() => {
              if (selectedTaskId) {
                void retryTask(selectedTaskId).then(handleRefreshAfterAction);
              }
            }}
            onRestart={() => {
              if (selectedTaskId) {
                void restartTask(selectedTaskId).then(handleRefreshAfterAction);
              }
            }}
          />
        </section>

        <section className="panel" aria-labelledby="task-history-title">
          <h2 id="task-history-title">Task History</h2>
          <TaskHistoryList
            tasks={tasks}
            selectedTaskId={selectedTaskId}
            onSelect={(taskId) => setSelectedTaskId(taskId)}
          />
        </section>
      </section>
    </main>
  );
}
