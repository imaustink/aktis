package vectorstore

import (
	"testing"

	"github.com/qdrant/go-client/qdrant"

	"github.com/aktisai/orchestrator/engines/temporal/internal/access"
)

var reader = access.Viewer{Roles: []string{"reader"}}

// The point of `hidden` (agent-controller ADR 0039 §2): a knowledge base's
// generated tools are reachable because the skill that owns them declared them
// by id, and are kept out of open retrieval so every client's scoped tooling
// does not compete in front of every caller.
//
// These assert the two halves of that split directly, since exercising the
// filters through a live Qdrant needs the integration test's server.
func TestQueryFilterExcludesHiddenRecords(t *testing.T) {
	filter := queryFilter(reader)

	if len(filter.MustNot) != 1 {
		t.Fatalf("expected one must-not condition, got %d", len(filter.MustNot))
	}
	match := filter.MustNot[0].GetField()
	if match == nil || match.Key != "hidden" {
		t.Fatalf("expected the must-not to key on `hidden`, got %+v", filter.MustNot[0])
	}
	if !match.GetMatch().GetBoolean() {
		t.Error("expected the must-not to exclude hidden=true")
	}

	// The visibility clauses still have to be there — hiding must not replace
	// RBAC or private scoping.
	if len(filter.Must) != 2 {
		t.Errorf("expected the role and principal clauses to survive, got %d", len(filter.Must))
	}
}

func TestGetByIDsFilterKeepsHiddenRecordsReachable(t *testing.T) {
	// visibilityFilter is what GetByIDs uses. If it excluded hidden records,
	// a selected knowledge base would resolve none of its own declared tools.
	filter := visibilityFilter(reader)

	if len(filter.MustNot) != 0 {
		t.Errorf("id lookups must not exclude hidden records, got %+v", filter.MustNot)
	}
}

// ADR 0060: the principal clause admits public records (no `private: true`,
// which also covers records written before the field existed) plus private
// records naming one of the viewer's principals.
func TestVisibilityFilterPrincipalClause(t *testing.T) {
	principalClause := func(v access.Viewer) []*qdrant.Condition {
		filter := visibilityFilter(v)
		if len(filter.Must) != 2 {
			t.Fatalf("expected role AND principal clauses, got %+v", filter.Must)
		}
		return filter.Must[1].GetFilter().GetShould()
	}

	anonymous := principalClause(reader)
	if len(anonymous) != 1 {
		t.Fatalf("a viewer with no principals must match public records only, got %+v", anonymous)
	}
	publicOnly := anonymous[0].GetFilter().GetMustNot()
	if len(publicOnly) != 1 || publicOnly[0].GetField().GetKey() != "private" ||
		!publicOnly[0].GetField().GetMatch().GetBoolean() {
		t.Fatalf("expected must-not private=true, got %+v", anonymous[0])
	}

	named := principalClause(access.Viewer{Roles: reader.Roles, Principals: []string{"openwebui:alice"}})
	if len(named) != 2 {
		t.Fatalf("expected public OR names-me, got %+v", named)
	}
	field := named[1].GetField()
	if field.GetKey() != "principals" ||
		len(field.GetMatch().GetKeywords().GetStrings()) != 1 ||
		field.GetMatch().GetKeywords().GetStrings()[0] != "openwebui:alice" {
		t.Fatalf("expected a principals keyword match, got %+v", named[1])
	}
}

func TestAudienceOfRoundTripsPayload(t *testing.T) {
	payload := qdrant.NewValueMap(map[string]any{
		"roles":        []any{"eng"},
		"unrestricted": false,
		"private":      true,
		"principals":   []any{"openwebui:alice"},
	})
	got := audienceOf(payload)
	if !got.Private || len(got.Principals) != 1 || got.Principals[0] != "openwebui:alice" || got.Roles[0] != "eng" {
		t.Fatalf("audienceOf = %+v", got)
	}

	// A point written before ADR 0060 has neither field and reads as public.
	legacy := audienceOf(qdrant.NewValueMap(map[string]any{"roles": []any{"eng"}}))
	if legacy.Private {
		t.Fatal("a legacy point must read as public")
	}
}
