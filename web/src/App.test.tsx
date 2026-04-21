import { render, screen } from "@testing-library/react";
import App from "./App";

test("renders the local spark history ui shell", () => {
  render(<App />);

  expect(screen.getByRole("heading", { name: /spark history ui loader/i })).toBeInTheDocument();
  expect(screen.getByRole("button", { name: /upload file/i })).toBeInTheDocument();
  expect(screen.getByRole("button", { name: /local path/i })).toBeInTheDocument();
  expect(screen.getByText(/recommended for very large eventlogs/i)).toBeInTheDocument();
});
