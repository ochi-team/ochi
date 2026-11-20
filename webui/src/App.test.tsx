import { render, screen } from "@solidjs/testing-library";
import { flush } from "solid-js";
import { expect, test } from "vitest";
import App from "./App";

test("home renders on the first pass without a loading fallback", () => {
  render(() => <App />);
  flush();

  expect(screen.queryByText("Loading…")).not.toBeInTheDocument();
  expect(screen.getByRole("textbox")).toBeVisible();
  expect(screen.getByRole("button", { name: /^Run/ })).toBeDisabled();
});
