import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import request from "supertest";
import { createApp } from "./app";
import { TaskStore } from "./task-store";

test("creates a path-based task and returns the stable task dto", async () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "spark-ui-service-web-"));
  const eventLogPath = path.join(tempRoot, "sample.eventlog");
  fs.writeFileSync(eventLogPath, '{"Event":"SparkListenerLogStart","Spark Version":"3.3.0"}\n');

  const app = createApp({ workspaceRoot: path.join(tempRoot, ".local-runs") });

  const createResponse = await request(app)
    .post("/api/tasks")
    .send({
      inputMode: "path",
      eventLogPath,
      outputMode: "managed"
    });

  expect(createResponse.status).toBe(201);
  expect(createResponse.body.task).toMatchObject({
    status: "queued",
    inputMode: "path",
    outputMode: "managed",
    sourceLabel: "sample.eventlog"
  });
  expect(createResponse.body.task.workspaceDir).toBeUndefined();
  expect(createResponse.body.task.sourcePath).toBeUndefined();

  const listResponse = await request(app).get("/api/tasks");
  expect(listResponse.status).toBe(200);
  expect(listResponse.body.tasks).toHaveLength(1);
  expect(listResponse.body.tasks[0]).toMatchObject({
    id: createResponse.body.task.id,
    status: "queued",
    inputMode: "path",
    outputMode: "managed",
    sourceLabel: "sample.eventlog"
  });
});

test("persists custom output directories while keeping task indexing under the managed root", async () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "spark-ui-service-web-"));
  const workspaceRoot = path.join(tempRoot, ".local-runs");
  const customOutputDir = path.join(tempRoot, "custom-output");
  const app = createApp({ workspaceRoot });

  const createResponse = await request(app)
    .post("/api/tasks")
    .send({
      inputMode: "path",
      eventLogPath: createReadableEventLog(tempRoot, "custom.eventlog"),
      outputMode: "custom",
      outputDir: customOutputDir
    });

  expect(createResponse.status).toBe(201);
  const task = new TaskStore(workspaceRoot).get(createResponse.body.task.id);
  expect(task).toMatchObject({
    outputMode: "custom",
    outputDir: customOutputDir,
    workspaceDir: customOutputDir
  });
});

test("accepts upload tasks and returns task details", async () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "spark-ui-service-web-"));
  const uploadPath = path.join(tempRoot, "upload.eventlog");
  fs.writeFileSync(uploadPath, '{"Event":"SparkListenerLogStart","Spark Version":"3.3.0"}\n');

  const app = createApp({ workspaceRoot: path.join(tempRoot, ".local-runs") });

  const createResponse = await request(app)
    .post("/api/tasks")
    .field("inputMode", "upload")
    .attach("eventLogFile", uploadPath);

  expect(createResponse.status).toBe(201);
  expect(createResponse.body.task.inputMode).toBe("upload");
  expect(createResponse.body.task.outputMode).toBe("managed");
  expect(createResponse.body.task.sourceLabel).toBe("upload.eventlog");

  const detailResponse = await request(app).get(`/api/tasks/${createResponse.body.task.id}`);
  expect(detailResponse.status).toBe(200);
  expect(detailResponse.body.task.id).toBe(createResponse.body.task.id);
  expect(detailResponse.body.task.outputMode).toBe("managed");
  expect(detailResponse.body.task.uploadedFileName).toBe("upload.eventlog");
  expect(detailResponse.body.task.sourcePath).toBeUndefined();
  expect(detailResponse.body.task.workspaceDir).toBeUndefined();

  const logsResponse = await request(app).get(`/api/tasks/${createResponse.body.task.id}/logs`);
  expect(logsResponse.status).toBe(200);
  expect(logsResponse.body.logs.preprocessor).toBe("");
  expect(logsResponse.body.logs.historyServer).toBe("");
});

test("returns persisted task log files", async () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "spark-ui-service-web-"));
  const workspaceRoot = path.join(tempRoot, ".local-runs");
  const store = new TaskStore(workspaceRoot);
  const preprocessorLogPath = path.join(tempRoot, "preprocessor.log");
  const historyServerLogPath = path.join(tempRoot, "history-server.log");
  fs.writeFileSync(preprocessorLogPath, "preprocessor-output");
  fs.writeFileSync(historyServerLogPath, "history-server-output");

  const task = store.create({
    status: "queued",
    inputMode: "path",
    outputMode: "managed",
    sourceLabel: "sample.eventlog",
    sourcePath: createReadableEventLog(tempRoot, "sample.eventlog"),
    appId: undefined,
    historyUrl: undefined
  });
  store.update(task.id, {
    preprocessorLogPath,
    historyServerLogPath
  });

  const app = createApp({ workspaceRoot });
  const logsResponse = await request(app).get(`/api/tasks/${task.id}/logs`);
  expect(logsResponse.status).toBe(200);
  expect(logsResponse.body.logs.preprocessor).toBe("preprocessor-output");
  expect(logsResponse.body.logs.historyServer).toBe("history-server-output");
});

test("returns persisted runtime metadata in task details", async () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "spark-ui-service-web-"));
  const workspaceRoot = path.join(tempRoot, ".local-runs");
  const store = new TaskStore(workspaceRoot);

  const task = store.create({
    status: "ready",
    inputMode: "path",
    outputMode: "managed",
    sourceLabel: "sample.eventlog",
    sourcePath: createReadableEventLog(tempRoot, "sample.eventlog"),
    appId: "app-123",
    historyUrl: "http://127.0.0.1:18081/history/app-123/jobs/"
  });
  const updatedTask = store.update(task.id, {
    attemptId: "attempt-1",
    uimetaPath: "/tmp/app-123_attempt-1.uimeta",
    historyPort: 18081,
    executionStartedAt: "2026-04-22T00:00:00.000Z",
    preprocessorPid: 101,
    historyServerPid: 202,
    preprocessorLogPath: "/tmp/preprocessor.log",
    historyServerLogPath: "/tmp/history.log",
    preprocessorCommand: ["spark-submit", "--class", "org.apache.spark.deploy.history.EventLogPreprocessor"],
    historyServerCommand: ["spark-class", "org.apache.spark.deploy.history.HistoryServer"],
    historyServerState: "ready",
    errorSummary: "none"
  });

  const app = createApp({ workspaceRoot });
  const detailResponse = await request(app).get(`/api/tasks/${task.id}`);
  expect(detailResponse.status).toBe(200);
  expect(detailResponse.body.task).toMatchObject({
    id: task.id,
    updatedAt: updatedTask.updatedAt,
    historyUrl: `/history-proxy/${task.id}/history/app-123/jobs/`,
    attemptId: "attempt-1",
    uimetaPath: "/tmp/app-123_attempt-1.uimeta",
    historyPort: 18081,
    executionStartedAt: "2026-04-22T00:00:00.000Z",
    preprocessorPid: 101,
    historyServerPid: 202,
    preprocessorLogPath: "/tmp/preprocessor.log",
    historyServerLogPath: "/tmp/history.log",
    historyServerState: "ready",
    errorSummary: "none"
  });
});

test("proxies spark history html and rewrites absolute asset paths", async () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "spark-ui-service-web-"));
  const workspaceRoot = path.join(tempRoot, ".local-runs");
  const store = new TaskStore(workspaceRoot);
  const upstream = await startMockHistoryServer();

  const task = store.create({
    status: "ready",
    inputMode: "path",
    outputMode: "managed",
    sourceLabel: "sample.eventlog",
    sourcePath: createReadableEventLog(tempRoot, "sample.eventlog"),
    appId: "app-123",
    historyUrl: "/history-proxy/task-under-test/history/app-123/jobs/"
  });
  store.update(task.id, {
    historyPort: upstream.port,
    historyServerState: "ready"
  });

  const app = createApp({ workspaceRoot });
  const response = await request(app).get(`/history-proxy/${task.id}/history/app-123/jobs/`);

  expect(response.status).toBe(200);
  expect(response.text).toContain(`href="/history-proxy/${task.id}/static/webui.css"`);
  expect(response.text).toContain(`src="/history-proxy/${task.id}/static/webui.js"`);
  expect(response.text).toContain(`setUIRoot('/history-proxy/${task.id}')`);
  expect(response.text).toContain(`setAppBasePath('/history-proxy/${task.id}/history/app-123')`);

  await stopMockHistoryServer(upstream.server);
});

test("proxies spark history static assets through the task route", async () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "spark-ui-service-web-"));
  const workspaceRoot = path.join(tempRoot, ".local-runs");
  const store = new TaskStore(workspaceRoot);
  const upstream = await startMockHistoryServer();

  const task = store.create({
    status: "ready",
    inputMode: "path",
    outputMode: "managed",
    sourceLabel: "sample.eventlog",
    sourcePath: createReadableEventLog(tempRoot, "sample.eventlog"),
    appId: "app-123",
    historyUrl: "/history-proxy/task-under-test/history/app-123/jobs/"
  });
  store.update(task.id, {
    historyPort: upstream.port,
    historyServerState: "ready"
  });

  const app = createApp({ workspaceRoot });
  const response = await request(app).get(`/history-proxy/${task.id}/static/webui.css`);

  expect(response.status).toBe(200);
  expect(response.text).toBe("body { color: rgb(1, 2, 3); }");
  expect(response.headers["content-type"]).toMatch(/text\/css/);

  await stopMockHistoryServer(upstream.server);
});

test("cleans up staged uploads when validation fails", async () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "spark-ui-service-web-"));
  const uploadPath = path.join(tempRoot, "bad-upload.eventlog");
  fs.writeFileSync(uploadPath, '{"Event":"SparkListenerLogStart","Spark Version":"3.3.0"}\n');

  const workspaceRoot = path.join(tempRoot, ".local-runs");
  const app = createApp({ workspaceRoot });

  const response = await request(app)
    .post("/api/tasks")
    .field("inputMode", "upload")
    .field("outputMode", "invalid")
    .attach("eventLogFile", uploadPath);

  expect(response.status).toBe(400);
  const uploadsDir = path.join(workspaceRoot, ".uploads");
  const stagedFiles = fs.existsSync(uploadsDir) ? fs.readdirSync(uploadsDir) : [];
  expect(stagedFiles).toEqual([]);
});

test("cleans up staged uploads for malformed multipart task shapes", async () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "spark-ui-service-web-"));
  const uploadPath = path.join(tempRoot, "mixed.eventlog");
  fs.writeFileSync(uploadPath, '{"Event":"SparkListenerLogStart","Spark Version":"3.3.0"}\n');

  const workspaceRoot = path.join(tempRoot, ".local-runs");
  const app = createApp({ workspaceRoot });

  const response = await request(app)
    .post("/api/tasks")
    .field("inputMode", "path")
    .field("eventLogPath", "/tmp/does-not-matter.eventlog")
    .attach("eventLogFile", uploadPath);

  expect(response.status).toBe(400);
  const uploadsDir = path.join(workspaceRoot, ".uploads");
  const stagedFiles = fs.existsSync(uploadsDir) ? fs.readdirSync(uploadsDir) : [];
  expect(stagedFiles).toEqual([]);
});

test("cleans up staged uploads after a successful path-mode multipart request", async () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "spark-ui-service-web-"));
  const uploadPath = path.join(tempRoot, "path-plus-upload.eventlog");
  fs.writeFileSync(uploadPath, '{"Event":"SparkListenerLogStart","Spark Version":"3.3.0"}\n');
  const eventLogPath = path.join(tempRoot, "path.eventlog");
  fs.writeFileSync(eventLogPath, '{"Event":"SparkListenerLogStart","Spark Version":"3.3.0"}\n');

  const workspaceRoot = path.join(tempRoot, ".local-runs");
  const app = createApp({ workspaceRoot });

  const response = await request(app)
    .post("/api/tasks")
    .field("inputMode", "path")
    .field("eventLogPath", eventLogPath)
    .attach("eventLogFile", uploadPath);

  expect(response.status).toBe(201);
  expect(response.body.task.inputMode).toBe("path");
  expect(response.body.task.outputMode).toBe("managed");

  const uploadsDir = path.join(workspaceRoot, ".uploads");
  const stagedFiles = fs.existsSync(uploadsDir) ? fs.readdirSync(uploadsDir) : [];
  expect(stagedFiles).toEqual([]);
});

test("rejects invalid output mode", async () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "spark-ui-service-web-"));
  const eventLogPath = path.join(tempRoot, "sample.eventlog");
  fs.writeFileSync(eventLogPath, "ok\n");

  const app = createApp({ workspaceRoot: path.join(tempRoot, ".local-runs") });

  const response = await request(app)
    .post("/api/tasks")
    .send({
      inputMode: "path",
      eventLogPath,
      outputMode: "invalid"
  });

  expect(response.status).toBe(400);
  expect(response.body.error).toMatch(/outputMode/i);
});

test("rejects custom mode requests without an absolute output directory", async () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "spark-ui-service-web-"));
  const app = createApp({ workspaceRoot: path.join(tempRoot, ".local-runs") });

  const response = await request(app)
    .post("/api/tasks")
    .send({
      inputMode: "path",
      eventLogPath: createReadableEventLog(tempRoot, "custom.eventlog"),
      outputMode: "custom"
    });

  expect(response.status).toBe(400);
  expect(response.body.error).toMatch(/outputDir/i);
});

test("rejects unreadable eventlog paths", async () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "spark-ui-service-web-"));
  const unreadablePath = path.join(tempRoot, "missing.eventlog");

  const app = createApp({ workspaceRoot: path.join(tempRoot, ".local-runs") });

  const response = await request(app)
    .post("/api/tasks")
    .send({
      inputMode: "path",
      eventLogPath: unreadablePath,
      outputMode: "managed"
    });

  expect(response.status).toBe(400);
});

test("skips malformed task directories when listing", async () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "spark-ui-service-web-"));
  const workspaceRoot = path.join(tempRoot, ".local-runs");
  const app = createApp({ workspaceRoot });

  const validResponse = await request(app)
    .post("/api/tasks")
    .send({
      inputMode: "path",
      eventLogPath: createReadableEventLog(tempRoot, "valid.eventlog"),
      outputMode: "managed"
    });

  const badTaskDir = path.join(workspaceRoot, "broken-task");
  fs.mkdirSync(badTaskDir, { recursive: true });
  fs.writeFileSync(path.join(badTaskDir, "task.json"), "{");

  const partialTaskDir = path.join(workspaceRoot, "partial-task");
  fs.mkdirSync(partialTaskDir, { recursive: true });
  fs.writeFileSync(path.join(partialTaskDir, "task.json"), JSON.stringify({ id: "partial-task" }));

  const listResponse = await request(app).get("/api/tasks");
  expect(listResponse.status).toBe(200);
  expect(listResponse.body.tasks).toHaveLength(1);
  expect(listResponse.body.tasks[0].id).toBe(validResponse.body.task.id);
});

test("skips invalid persisted tasks and emits a warning", () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "spark-ui-service-web-"));
  const workspaceRoot = path.join(tempRoot, ".local-runs");
  const store = new TaskStore(workspaceRoot);
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

  const taskDir = path.join(workspaceRoot, "bad-task");
  fs.mkdirSync(taskDir, { recursive: true });
  fs.writeFileSync(
    path.join(taskDir, "task.json"),
    JSON.stringify({
      id: "bad-task",
      status: "bogus",
      inputMode: "path",
      outputMode: "managed",
      sourceLabel: "bad.eventlog",
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      workspaceDir: taskDir
    })
  );

  expect(store.list()).toEqual([]);
  expect(warnSpy).toHaveBeenCalled();

  warnSpy.mockRestore();
});

test("reads legacy persisted tasks without outputMode and workspaceDir", () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "spark-ui-service-web-"));
  const workspaceRoot = path.join(tempRoot, ".local-runs");
  const store = new TaskStore(workspaceRoot);

  const taskDir = path.join(workspaceRoot, "legacy-task");
  fs.mkdirSync(taskDir, { recursive: true });
  fs.writeFileSync(
    path.join(taskDir, "task.json"),
    JSON.stringify({
      id: "legacy-task",
      status: "queued",
      inputMode: "path",
      sourceLabel: "legacy.eventlog",
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    })
  );

  expect(store.get("legacy-task")).toMatchObject({
    id: "legacy-task",
    outputMode: "managed",
    workspaceDir: taskDir
  });
  expect(store.list()[0]).toMatchObject({
    id: "legacy-task",
    outputMode: "managed",
    workspaceDir: taskDir
  });
});

test("stops and retries a task through control routes", async () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "spark-ui-service-web-"));
  const eventLogPath = createReadableEventLog(tempRoot, "sample.eventlog");
  const stop = vi.fn(async () => undefined);
  const app = createApp({
    workspaceRoot: path.join(tempRoot, ".local-runs"),
    taskExecutor: {
      enqueue: async () => ({
        status: "ready",
        appId: "local-123",
        historyUrl: "http://127.0.0.1:18081/history/local-123/jobs/"
      }),
      stop
    }
  });

  const createResponse = await request(app)
    .post("/api/tasks")
    .send({
      inputMode: "path",
      eventLogPath,
      outputMode: "managed"
    });

  expect(createResponse.status).toBe(201);

  const stopResponse = await request(app).post(`/api/tasks/${createResponse.body.task.id}/stop`);
  expect(stopResponse.status).toBe(200);
  expect(stopResponse.body.task.status).toBe("stopped");

  const retryResponse = await request(app).post(`/api/tasks/${createResponse.body.task.id}/retry`);
  expect(retryResponse.status).toBe(200);
  expect(retryResponse.body.task.status).toBe("ready");
  expect(retryResponse.body.task.historyUrl).toContain("18081");
  expect(stop).toHaveBeenCalledTimes(2);
});

test("marks the task failed when retry execution rejects", async () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "spark-ui-service-web-"));
  const eventLogPath = createReadableEventLog(tempRoot, "sample.eventlog");
  let enqueueCalls = 0;
  const app = createApp({
    workspaceRoot: path.join(tempRoot, ".local-runs"),
    taskExecutor: {
      enqueue: async () => {
        enqueueCalls += 1;
        if (enqueueCalls === 1 || enqueueCalls === 2) {
          throw new Error(enqueueCalls === 1 ? "initial boom" : "boom");
        }
        throw new Error("unexpected");
      },
      stop: async () => undefined
    }
  });

  const createResponse = await request(app)
    .post("/api/tasks")
    .send({
      inputMode: "path",
      eventLogPath,
      outputMode: "managed"
    });

  await waitForAsyncWork();
  const retryResponse = await request(app).post(`/api/tasks/${createResponse.body.task.id}/retry`);
  expect(retryResponse.status).toBe(500);
  expect(retryResponse.body.task.status).toBe("failed");

  const detailResponse = await request(app).get(`/api/tasks/${createResponse.body.task.id}`);
  expect(detailResponse.status).toBe(200);
  expect(detailResponse.body.task.status).toBe("failed");
});

test("rejects retry for tasks that are already ready", async () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "spark-ui-service-web-"));
  const eventLogPath = createReadableEventLog(tempRoot, "sample.eventlog");
  const app = createApp({
    workspaceRoot: path.join(tempRoot, ".local-runs"),
    taskExecutor: {
      enqueue: async () => ({
        status: "ready",
        appId: "ready-app",
        historyUrl: "http://127.0.0.1:18081/history/ready-app/jobs/"
      }),
      stop: async () => undefined
    }
  });

  const createResponse = await request(app)
    .post("/api/tasks")
    .send({
      inputMode: "path",
      eventLogPath,
      outputMode: "managed"
    });

  await waitForAsyncWork();
  const retryResponse = await request(app).post(`/api/tasks/${createResponse.body.task.id}/retry`);
  expect(retryResponse.status).toBe(409);
  expect(retryResponse.body.task.status).toBe("ready");
});

test("restarts a ready task service", async () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "spark-ui-service-web-"));
  const eventLogPath = createReadableEventLog(tempRoot, "sample.eventlog");
  const stop = vi.fn(async () => undefined);
  const app = createApp({
    workspaceRoot: path.join(tempRoot, ".local-runs"),
    taskExecutor: {
      enqueue: async () => ({
        status: "ready",
        appId: "restarted-app",
        historyUrl: "http://127.0.0.1:18083/history/restarted-app/jobs/"
      }),
      stop
    }
  });

  const createResponse = await request(app)
    .post("/api/tasks")
    .send({
      inputMode: "path",
      eventLogPath,
      outputMode: "managed"
    });

  await waitForAsyncWork();
  const restartResponse = await request(app).post(`/api/tasks/${createResponse.body.task.id}/restart`);
  expect(restartResponse.status).toBe(200);
  expect(restartResponse.body.task.status).toBe("ready");
  expect(restartResponse.body.task.historyUrl).toContain("restarted-app");
  expect(stop).toHaveBeenCalledTimes(1);
});

test("ignores stale enqueue results after stop and retry start a newer run", async () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "spark-ui-service-web-"));
  const eventLogPath = createReadableEventLog(tempRoot, "sample.eventlog");
  const firstRun = createDeferred<{
    status: "ready";
    appId: string;
    historyUrl: string;
  }>();
  const secondRun = createDeferred<{
    status: "ready";
    appId: string;
    historyUrl: string;
  }>();
  let enqueueCalls = 0;

  const app = createApp({
    workspaceRoot: path.join(tempRoot, ".local-runs"),
    taskExecutor: {
      enqueue: async () => {
        enqueueCalls += 1;
        return enqueueCalls === 1 ? firstRun.promise : secondRun.promise;
      },
      stop: async () => undefined
    }
  });

  const createResponse = await request(app)
    .post("/api/tasks")
    .send({
      inputMode: "path",
      eventLogPath,
      outputMode: "managed"
    });

  const stopResponse = await request(app).post(`/api/tasks/${createResponse.body.task.id}/stop`);
  expect(stopResponse.status).toBe(200);

  const retryRequest = request(app).post(`/api/tasks/${createResponse.body.task.id}/retry`);
  await waitForAsyncWork();

  secondRun.resolve({
    status: "ready",
    appId: "retry-app",
    historyUrl: "http://127.0.0.1:18082/history/retry-app/jobs/"
  });
  const retryResponse = await retryRequest;

  firstRun.resolve({
    status: "ready",
    appId: "stale-app",
    historyUrl: "http://127.0.0.1:18081/history/stale-app/jobs/"
  });
  await waitForAsyncWork();

  expect(retryResponse.status).toBe(200);
  expect(retryResponse.body.task.appId).toBe("retry-app");

  const detailResponse = await request(app).get(`/api/tasks/${createResponse.body.task.id}`);
  expect(detailResponse.status).toBe(200);
  expect(detailResponse.body.task.appId).toBe("retry-app");
  expect(detailResponse.body.task.historyUrl).toContain("retry-app");
});

function createReadableEventLog(tempRoot: string, fileName: string) {
  const eventLogPath = path.join(tempRoot, fileName);
  fs.writeFileSync(eventLogPath, '{"Event":"SparkListenerLogStart","Spark Version":"3.3.0"}\n');
  return eventLogPath;
}

function createDeferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((resolver) => {
    resolve = resolver;
  });
  return { promise, resolve };
}

async function waitForAsyncWork() {
  await new Promise((resolve) => setTimeout(resolve, 0));
}

async function startMockHistoryServer() {
  const server = http.createServer((req, res) => {
    if (req.url === "/history/app-123/jobs/") {
      res.setHeader("Content-Type", "text/html; charset=utf-8");
      res.end(`<!DOCTYPE html>
<html>
  <head>
    <link rel="stylesheet" href="/static/webui.css" />
    <script src="/static/webui.js"></script>
    <script>setUIRoot('')</script>
    <script>setAppBasePath('/history/app-123')</script>
  </head>
  <body>jobs</body>
</html>`);
      return;
    }

    if (req.url === "/static/webui.css") {
      res.setHeader("Content-Type", "text/css; charset=utf-8");
      res.end("body { color: rgb(1, 2, 3); }");
      return;
    }

    if (req.url === "/static/webui.js") {
      res.setHeader("Content-Type", "application/javascript; charset=utf-8");
      res.end("console.log('spark');");
      return;
    }

    res.statusCode = 404;
    res.end("missing");
  });

  await new Promise<void>((resolve) => {
    server.listen(0, "127.0.0.1", () => resolve());
  });
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("Failed to determine the mock history server port.");
  }

  return { server, port: address.port };
}

async function stopMockHistoryServer(server: http.Server) {
  await new Promise<void>((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}
