import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["src/**/*.test.ts"],
    environment: "node",
    // The run tests go through a real Temporal (src/__fixtures__/temporal.ts).
    testTimeout: 30_000,
    hookTimeout: 60_000,
    // One file at a time: the Temporal dev server picks free ports for its
    // services and binds them a moment later, and on macOS a test server
    // listening on port 0 in a parallel file can be handed one of them in
    // between -- its requests to 127.0.0.1 then reach Temporal's HTTP API
    // and 404. Every other file here takes well under a second.
    fileParallelism: false,
  },
});
