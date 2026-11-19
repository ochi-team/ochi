import solid from "@solidjs/vite-plugin";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [solid()],
  resolve: { tsconfigPaths: true },
  test: {
    globals: false,
    projects: [
      {
        extends: true,
        test: {
          name: "components",
          environment: "happy-dom",
          include: ["**/*.test.tsx"],
          setupFiles: ["./tests/setup.ts"],
        },
      },
      {
        extends: true,
        test: {
          name: "node",
          environment: "node",
          include: ["**/*.test.ts"],
        },
      },
    ],
  },
});
