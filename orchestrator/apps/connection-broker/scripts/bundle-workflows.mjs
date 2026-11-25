// Bundles the compiled Corpus sync workflow (dist/sync/workflows.js) into the one
// file the Temporal worker loads (dist/sync/workflow-bundle.js), so a pod
// never runs webpack at startup -- slow, memory-hungry, and impossible on a
// read-only root filesystem. Runs after `tsc` (npm run build).
import { writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { bundleWorkflowCode } from "@temporalio/worker";

const workflowsPath = fileURLToPath(new URL("../dist/sync/workflows.js", import.meta.url));
const out = fileURLToPath(new URL("../dist/sync/workflow-bundle.js", import.meta.url));

const { code } = await bundleWorkflowCode({ workflowsPath });
await writeFile(out, code);
console.log(`bundled ${workflowsPath} -> ${out} (${Math.round(code.length / 1024)} KiB)`);
