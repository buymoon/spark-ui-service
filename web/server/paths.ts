import path from "node:path";

export function taskWorkspace(root: string, taskId: string) {
  return path.join(root, taskId);
}

export function taskFile(root: string, taskId: string, name: string) {
  return path.join(taskWorkspace(root, taskId), name);
}
