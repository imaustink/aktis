// Bundles the compiled run workflows (dist/runs/workflows.js) into the one
// file the Temporal worker loads (dist/runs/workflow-bundle.js), so a pod
// never runs webpack at startup -- slow, memory-hungry, and impossible on a
// read-only root filesystem. Runs after `tsc` (npm run build).
import { writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { bundleWorkflowCode } from "@temporalio/worker";

const workflowsPath = fileURLToPath(new URL("../dist/runs/workflows.js", import.meta.url));
const out = fileURLToPath(new URL("../dist/runs/workflow-bundle.js", import.meta.url));

const { code } = await bundleWorkflowCode({ workflowsPath });
await writeFile(out, code);
console.log(`bundled ${workflowsPath} -> ${out} (${Math.round(code.length / 1024)} KiB)`);
