package corpussync

// IndexedChunk is one chunk already in the corpus, as the store reports it.
type IndexedChunk struct {
	ContentHash string
	SourceID    string
}

// Scope is which resources a pass actually looked at, and so where deletion
// is safe. Getting it wrong empties a corpus: a full reconcile walked
// everything, so anything indexed and absent is gone; a webhook pass looked
// at a handful, and everything else is indexed-and-absent too.
type Scope struct {
	// Full: every resource was examined.
	Full bool
	// SourceIDs a partial pass fetched; only their chunks may be deleted.
	SourceIDs []string
}

// Plan is what a pass writes.
type Plan struct {
	// Upsert holds new or changed chunks, the only ones that cost an embedding.
	Upsert []Chunk
	// Remove holds content hashes to delete.
	Remove []string
	// Unchanged counts chunks already present and identical.
	Unchanged int
}

// Reconcile works out a pass's writes. Point ids are content hashes
// (docs/adr/0039 §7), so "has this chunk changed?" is a set difference and
// needs no stored state beyond the ids already indexed.
func Reconcile(current []Chunk, indexed []IndexedChunk, scope Scope) Plan {
	indexedHashes := make(map[string]bool, len(indexed))
	for _, c := range indexed {
		indexedHashes[c.ContentHash] = true
	}

	var plan Plan
	seen := map[string]bool{}
	for _, c := range current {
		// The same passage twice in one document is one point.
		if seen[c.ContentHash] {
			continue
		}
		seen[c.ContentHash] = true
		if indexedHashes[c.ContentHash] {
			plan.Unchanged++
		} else {
			plan.Upsert = append(plan.Upsert, c)
		}
	}

	touched := map[string]bool{}
	for _, id := range scope.SourceIDs {
		touched[id] = true
	}
	for _, c := range indexed {
		if (scope.Full || touched[c.SourceID]) && !seen[c.ContentHash] {
			plan.Remove = append(plan.Remove, c.ContentHash)
		}
	}
	return plan
}
