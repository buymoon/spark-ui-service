import path from "node:path";
import { createApp } from "./app";
import { createTaskExecutor } from "./task-executor";

const port = Number(process.env.PORT ?? 3000);
const workspaceRoot = process.env.WORKSPACE_ROOT ?? path.resolve(process.cwd(), ".local-runs");
const repoRoot = process.env.REPO_ROOT ?? path.resolve(process.cwd(), "..");
const taskExecutor = createTaskExecutor({
  workspaceRoot,
  repoRoot,
  sparkHome: process.env.SPARK_HOME
});

const app = createApp({ workspaceRoot, taskExecutor });

app.listen(port, () => {
  console.log(`spark-ui-service web dev server listening on ${port}`);
});
