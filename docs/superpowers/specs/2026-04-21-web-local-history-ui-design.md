# Web Local Spark History UI Design

## Summary

Build a local single-user web application under `web/` that accepts an open-source Spark eventlog through either file upload or an absolute local path, preprocesses it into a `.uimeta` snapshot, launches a task-scoped local Spark History Server, and lets the user open the native Spark History UI in a new browser tab.

The first release should optimize for large local files, stable task isolation, and minimal Spark-specific logic in the web layer. The web app should manage its own cache by default while still allowing users to override the output directory per task.

## Goals

- Provide a browser-based local workflow for turning a Spark eventlog into a directly loadable Spark History UI.
- Support both drag-and-drop / file-picker uploads and absolute local filesystem paths.
- Handle very large eventlogs efficiently, with path mode recommended for large files.
- Generate `.uimeta` snapshots through a dedicated offline preprocessing flow.
- Automatically start a local History Server for each completed task.
- Preserve task history so users can reopen prior runs, inspect logs, retry failures, stop services, and clean up outputs.
- Add an end-to-end test flow that can generate and validate a roughly 1 GB open-source Spark eventlog.

## Non-Goals

- Rebuild or redesign Spark History UI pages inside the web app.
- Support multi-user remote deployment in the first release.
- Build a shared multi-tenant History Server for many tasks in the first release.
- Add native OS file-picker integrations beyond standard browser upload.
- Guarantee cross-version compatibility with every Spark release beyond the repository's supported baseline without explicit validation.

## User Experience

### Primary workflow

1. The user opens the local web app under `web/`.
2. The user chooses one input mode:
   - upload an eventlog file via drag-and-drop or file picker
   - enter an absolute local path to an eventlog file
3. The user optionally accepts the default managed output location or specifies a custom output directory.
4. The user submits the task.
5. The app validates the input, creates a task workspace, preprocesses the eventlog into `.uimeta`, starts a local History Server, and reports progress live.
6. When the task reaches `ready`, the app shows an `Open UI` action that opens the native Spark History UI in a new browser tab.

### First-release page structure

- `New Task`
  - input mode tabs: `Upload File` and `Local Path`
  - upload dropzone / file picker
  - absolute path input
  - output directory option: app-managed or custom
  - `Auto-open UI when ready` toggle
  - submit button
- `Current Task`
  - task status timeline
  - source metadata
  - generated artifact metadata
  - History Server URL
  - actions: `Open UI`, `Copy URL`, `View Logs`, `Stop Service`, `Retry`, `Delete Task`
- `Task History`
  - recent task cards with status, source, size, creation time
  - actions: `Open UI`, `Restart Service`, `View Logs`, `Delete`

### UX rules

- The UI should explicitly recommend local-path mode for very large files.
- The app should never embed the Spark UI in an iframe by default.
- Failures should include a concise error summary and expandable raw logs.
- Completed tasks should remain visible and reopenable until the user deletes them.

## Architecture

The implementation should live under a new `web/` directory and be split into:

- a local frontend SPA for task creation, progress display, and result access
- a local Node-based backend API for file handling, process management, and task persistence
- a new Scala offline preprocessor entry point that converts a Spark eventlog into `.uimeta`

### Responsibility split

#### Frontend

- collect user input
- submit tasks
- poll task state
- display logs and task history
- open the native Spark UI in a new tab

#### Node backend

- receive uploads and path-based task requests
- validate inputs and create task workspaces
- invoke the Scala preprocessor
- manage task status transitions
- allocate ports and start / stop local History Server processes
- expose task metadata and logs to the frontend

#### Scala preprocessor

- load and replay the eventlog using Spark-compatible logic
- build UI metadata state from the replayed status store
- write the `.uimeta` output
- emit machine-readable task result metadata such as `appId`, `attemptId`, and output paths

This keeps Spark-specific behavior in Scala while letting the web backend focus on local app orchestration.

## Task Workspace Layout

The application should manage a default workspace root under:

`web/.local-runs/`

Each task gets its own isolated directory:

`web/.local-runs/<taskId>/`

Recommended structure:

- `input/`
  - stores uploaded source files
  - stores source metadata for path-based tasks
- `events/`
  - normalized eventlog location used by History Server
  - upload mode copies the source file here
  - path mode prefers symlink, with copy fallback when needed
- `uimeta/`
  - generated `.uimeta` artifacts
- `history/`
  - History Server runtime files such as PID, port, launch logs, and temp state
- `task.json`
  - durable task metadata and current state

If the user selects a custom output directory, the same structure should be created there for that task while still registering the task in the app-managed index.

## Task Lifecycle

Recommended statuses:

- `queued`
- `validating`
- `preprocessing`
- `starting_history_server`
- `ready`
- `failed`
- `stopped`

### Lifecycle details

1. Backend receives task creation request.
2. Backend validates source existence, readability, and basic eventlog shape.
3. Backend creates the task workspace and writes initial `task.json`.
4. Backend normalizes the eventlog into the task `events/` directory.
5. Backend invokes the Scala preprocessor against that eventlog and task `uimeta/` directory.
6. On success, backend records `appId`, `attemptId`, `.uimeta` path, and sizes.
7. Backend starts a dedicated local History Server for the task using the task-local directories.
8. Backend marks the task `ready` once the server is reachable.
9. The frontend exposes `Open UI` using the task-specific History Server URL.

### Isolation model

The first release should use one History Server process per task.

Why:

- simpler failure isolation
- easier port and lifecycle management
- easier debugging for local single-user usage
- avoids early complexity around shared provider state

Tradeoff:

- multiple ready tasks can consume multiple ports and extra memory

This tradeoff is acceptable for the first release.

## API Design

### `POST /api/tasks`

Creates a new task.

Supported request forms:

- `multipart/form-data`
  - file upload
  - optional custom output directory
  - optional `autoOpen` flag
- `application/json`
  - absolute local eventlog path
  - optional custom output directory
  - optional `autoOpen` flag

Response should include:

- `taskId`
- initial status
- normalized input summary

### `GET /api/tasks`

Returns task list summaries ordered by most recent first.

### `GET /api/tasks/:id`

Returns full task details:

- status
- source description
- file sizes if known
- `appId`
- `.uimeta` metadata
- History Server URL if available
- timestamps
- error summary if failed

### `GET /api/tasks/:id/logs`

Returns preprocessing logs and History Server launch logs.

### `POST /api/tasks/:id/retry`

Retries a failed or stopped task using the same saved input definition.

### `POST /api/tasks/:id/stop`

Stops the History Server process for the task and updates state to `stopped`.

### `DELETE /api/tasks/:id`

Deletes the task workspace and terminates any associated running process.

## Offline Preprocessor Design

Add a new Scala entry point, tentatively named:

`org.apache.spark.deploy.history.EventLogPreprocessor`

### Inputs

- eventlog file path
- uimeta output directory
- optional machine-readable output path for result metadata

### Outputs

- final `.uimeta` file
- JSON metadata file or stdout JSON containing:
  - `appId`
  - `attemptId`
  - eventlog path
  - uimeta path
  - generated file sizes
  - elapsed preprocessing time

### Processing behavior

- replay the open-source Spark eventlog with Spark-compatible code
- reconstruct the needed status-store-backed metadata
- serialize the compact `.uimeta` snapshot using repository utilities
- fail clearly on malformed or unsupported inputs

The preprocessor should be designed for offline generation from existing logs rather than relying on the runtime listener path.

## History Server Management

For each successful task, the backend should start a local Spark History Server process with:

- `spark.history.fs.logDirectory` pointed at the task `events/` directory
- `spark.history.provider=org.apache.spark.deploy.history.UIMetaProvider`
- `spark.uimeta.dir` pointed at the task `uimeta/` directory

### Runtime metadata to persist

- assigned port
- PID or process handle metadata
- launch command
- start timestamp
- health status
- stdout / stderr log file paths

### Port strategy

- allocate from a local configurable range, such as starting near `18080`
- verify the selected port is free before launch
- persist the chosen port in `task.json`

### Reuse behavior

- if a task is already `ready` and the process is alive, reuse the existing server
- if a task is `ready` but the process died, surface a `Restart Service` action that recreates it from saved task metadata

## Large File Handling

- Path mode should be the recommended option for very large logs.
- Upload mode should remain available for convenience and smaller local files.
- The backend should stream uploads to disk rather than buffering entire files in memory.
- Path mode should avoid copying huge files when a symlink is safe and practical.
- Task status updates should remain responsive during long preprocessing runs.

## Error Handling

Failure categories should be reflected clearly in task status and logs:

- invalid path
- unreadable file
- non-eventlog input
- preprocessing failure
- Spark runtime / classpath startup failure
- History Server startup failure
- port allocation failure

For each failure, the task should retain:

- a concise user-facing summary
- raw log access
- enough saved input metadata to support retry without re-entering everything

## Testing Strategy

Testing should be split into fast coverage and heavyweight end-to-end validation.

### Fast tests

#### Scala

- preprocessor argument parsing
- preprocessor failure behavior on invalid paths
- preprocessor success path on a small known eventlog
- `.uimeta` generation assertions

#### Web backend

- task request validation
- upload vs path request handling
- task state transitions
- process manager behavior
- log retrieval and task persistence behavior

#### Frontend

- mode switching between upload and path inputs
- status rendering
- task actions visibility by state

### Heavyweight end-to-end test

Add a script such as:

`scripts/run_1gb_web_e2e.sh`

The script should:

1. generate an approximately 1 GB open-source Spark eventlog using the existing large-log generator
2. start the local `web/` backend
3. create a task through the backend using local-path mode
4. poll until the task reaches `ready`
5. verify:
   - `.uimeta` exists
   - task metadata contains `appId`
   - History Server URL is reachable
   - final task state is correct

This test should be opt-in and not run as part of the default quick test suite.

## Success Criteria

The first release is successful when:

- users can create a task from file upload
- users can create a task from an absolute local path
- the app preprocesses the eventlog into `.uimeta`
- the app starts a local History Server automatically after preprocessing
- the app exposes a working `Open UI` action that opens the native Spark History UI
- the app keeps task history and allows reopen / retry / stop / delete flows
- the 1 GB path-mode end-to-end test passes

## Risks And Follow-Up Considerations

- Spark internal status classes may differ across Spark versions, so replay compatibility should be validated carefully during implementation.
- One History Server per task may become resource-heavy if many tasks are left running; later work can add idle shutdown or shared-server modes.
- Path-mode symlink behavior can vary by filesystem and OS configuration, so copy fallback is required.
- Uploading very large files through the browser will be slower and less reliable than path mode; the UI should communicate that clearly.
