import path from "node:path";
import { createApp } from "./app";

const port = Number(process.env.PORT ?? 3000);
const workspaceRoot = process.env.WORKSPACE_ROOT ?? path.resolve(process.cwd(), ".local-runs");

const app = createApp({ workspaceRoot });

app.listen(port, () => {
  console.log(`spark-ui-service web dev server listening on ${port}`);
});
