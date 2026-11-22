/** Port for turning text into an embedding vector (the caller-tool cache's only need). */
export interface Embedder {
  embed(text: string): Promise<number[]>;
}
