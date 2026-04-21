import fs from "node:fs";
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

function createReadableEventLog(tempRoot: string, fileName: string) {
  const eventLogPath = path.join(tempRoot, fileName);
  fs.writeFileSync(eventLogPath, '{"Event":"SparkListenerLogStart","Spark Version":"3.3.0"}\n');
  return eventLogPath;
}
