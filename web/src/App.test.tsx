import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import App from "./App";

beforeEach(() => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url === "/api/tasks") {
        return new Response(JSON.stringify({ tasks: [] }), {
          status: 200,
          headers: { "Content-Type": "application/json" }
        });
      }

      if (url.startsWith("/api/tasks/") && url.endsWith("/logs")) {
        return new Response(JSON.stringify({ logs: { preprocessor: "", historyServer: "" } }), {
          status: 200,
          headers: { "Content-Type": "application/json" }
        });
      }

      if (url.startsWith("/api/tasks/")) {
        return new Response(
          JSON.stringify({
            task: {
              id: "task-1",
              status: "queued",
              inputMode: "path",
              sourceLabel: "sample.eventlog",
              createdAt: "2026-04-22T00:00:00.000Z",
              updatedAt: "2026-04-22T00:00:00.000Z"
            }
          }),
          {
            status: 200,
            headers: { "Content-Type": "application/json" }
          }
        );
      }

      throw new Error(`Unhandled fetch for ${url}`);
    })
  );
});

afterEach(() => {
  vi.unstubAllGlobals();
});

test("shows task creation, current task, and history sections", async () => {
  render(<App />);

  expect(screen.getByRole("heading", { name: /spark history ui loader/i })).toBeInTheDocument();
  expect(screen.getByRole("heading", { name: /new task/i })).toBeInTheDocument();
  expect(screen.getByRole("heading", { name: /current task/i })).toBeInTheDocument();
  expect(screen.getByRole("heading", { name: /task history/i })).toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: /local path/i }));
  await waitFor(() =>
    expect(screen.getByLabelText(/absolute eventlog path/i)).toBeInTheDocument()
  );
});
