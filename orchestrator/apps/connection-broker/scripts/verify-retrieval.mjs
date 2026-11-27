#!/usr/bin/env node
/**
 * Asks the indexed corpus real questions and shows what comes back.
 *
 * Everything else verifies PLUMBING: that chunks are written, hashes are
 * stable, boundaries hold. None of it can tell you whether the thing an agent
 * would retrieve is the thing it should have retrieved — a corpus can be
 * perfectly synced and useless, and it looks identical from the outside.
 *
 * This is the only check that needs real vectors.
 *
 *   node orchestrator/apps/connection-broker/scripts/verify-retrieval.mjs <collection> [question...]
 */
import { findEnvFile, loadEnv } from "./lib/env.mjs";

const env = loadEnv(findEnvFile());
const QDRANT = process.env.QDRANT_URL ?? "http://localhost:6333";
const COLLECTION = process.argv[2];
if (!COLLECTION) {
  console.error("usage: verify-retrieval.mjs <collection> [question...]");
  process.exit(1);
}

const questions = process.argv.slice(3);
if (questions.length === 0) {
  // Deliberately phrased as a person would ask, not as keywords: the point of
  // a vector index is that those differ.
  questions.push(
    "what went wrong on this project and what would we do differently",
    "who was on the team and what were their roles",
    "how do we track time and bill the client",
  );
}

// The model corpus-sync indexes with (docs/adr/0059): a query embedded with
// anything else scores against nothing meaningful.
const embedder = {
  async embed(texts) {
    const res = await fetch("https://api.openai.com/v1/embeddings", {
      method: "POST",
      headers: { authorization: `Bearer ${env.OPENAI_API_KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ model: process.env.EMBEDDING_MODEL ?? "text-embedding-3-small", input: texts }),
    });
    if (!res.ok) throw new Error(`embeddings ${res.status}: ${(await res.text()).slice(0, 300)}`);
    const { data } = await res.json();
    return [...data].sort((a, b) => a.index - b.index).map((d) => d.embedding);
  },
};

const info = await (await fetch(`${QDRANT}/collections/${COLLECTION}`)).json();
if (!info.result) {
  console.error(`no such collection: ${COLLECTION}`);
  process.exit(1);
}
console.log(`\nretrieval over ${COLLECTION} — ${info.result.points_count} point(s)\n`);

let weakest = 1;
for (const question of questions) {
  const [vector] = await embedder.embed([question]);

  const res = await fetch(`${QDRANT}/collections/${COLLECTION}/points/search`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ vector, limit: 3, with_payload: true }),
  });
  const { result } = await res.json();

  console.log(`? ${question}`);
  if (!result || result.length === 0) {
    console.log("  (nothing returned)\n");
    weakest = 0;
    continue;
  }

  for (const hit of result) {
    const d = typeof hit.payload.descriptor === "string"
      ? JSON.parse(hit.payload.descriptor)
      : hit.payload.descriptor;
    const text = (d.text ?? "").replace(/\s+/g, " ").trim();
    console.log(`  ${hit.score.toFixed(3)}  ${d.title}`);
    console.log(`         ${text.slice(0, 150)}`);
  }
  weakest = Math.min(weakest, result[0].score);
  console.log();
}

// A similarity floor, not a correctness proof. Cosine over
// text-embedding-3-small puts a genuinely relevant passage well above this;
// scores hovering near zero mean the index is answering with noise, which is
// what a broken embedding or a mis-stored vector looks like.
console.log(`weakest top hit: ${weakest.toFixed(3)}`);
if (weakest < 0.2) {
  console.error("\n✗ the best match for at least one question is near-noise");
  process.exit(1);
}
console.log("\nPASS — the corpus answers questions with relevant passages.");
