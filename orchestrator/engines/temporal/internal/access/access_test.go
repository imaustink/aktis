package access

import (
	"reflect"
	"testing"
)

func TestAdmits(t *testing.T) {
	alice := Viewer{Roles: []string{"eng"}, Principals: []string{"openwebui:alice"}}
	shared := Viewer{Roles: []string{"eng"}} // no principal: a shared/service subject

	cases := []struct {
		name   string
		viewer Viewer
		aud    Audience
		want   bool
	}{
		{"public, role held", alice, Of([]string{"eng"}, nil), true},
		{"public, role missing", alice, Of([]string{"sales"}, nil), false},
		{"private, listed and role held", alice, Of([]string{"eng"}, []string{"openwebui:alice"}), true},
		{"private, listed but role missing: principals never widen RBAC", alice, Of([]string{"sales"}, []string{"openwebui:alice"}), false},
		{"private, not listed", alice, Of([]string{"eng"}, []string{"openwebui:bob"}), false},
		{"private, viewer has no principal", shared, Of([]string{"eng"}, []string{"openwebui:alice"}), false},
		{"unrestricted public", shared, Audience{Unrestricted: true}, true},
		{"unrestricted but private still needs a principal", shared, Audience{Unrestricted: true, Private: true, Principals: []string{"openwebui:alice"}}, false},
		{"unrestricted private, listed", alice, Audience{Unrestricted: true, Private: true, Principals: []string{"openwebui:alice"}}, true},
		{"private with an empty set admits no one", alice, Audience{Roles: []string{"eng"}, Private: true}, false},
		{"no roles at all fails closed", Viewer{Principals: []string{"openwebui:alice"}}, Of([]string{"eng"}, nil), false},
	}
	for _, c := range cases {
		if got := c.viewer.Admits(c.aud); got != c.want {
			t.Errorf("%s: Admits = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestNarrow(t *testing.T) {
	private, set := Narrow(false, nil, false, nil)
	if private || set != nil {
		t.Fatalf("public ∩ public = (%v, %v), want public", private, set)
	}
	private, set = Narrow(false, nil, true, []string{"a", "b", "a"})
	if !private || !reflect.DeepEqual(set, []string{"a", "b"}) {
		t.Fatalf("public ∩ {a,b} = (%v, %v)", private, set)
	}
	private, set = Narrow(true, []string{"a", "b"}, false, nil)
	if !private || !reflect.DeepEqual(set, []string{"a", "b"}) {
		t.Fatalf("{a,b} ∩ public = (%v, %v): a public part must not widen", private, set)
	}
	private, set = Narrow(true, []string{"a", "b"}, true, []string{"c"})
	if !private || len(set) != 0 {
		t.Fatalf("disjoint = (%v, %v), want private and empty", private, set)
	}
}

func TestWiden(t *testing.T) {
	private, set := Widen([][]string{{"a"}, {"b", "a"}}, []bool{true, true})
	if !private || !reflect.DeepEqual(set, []string{"a", "b"}) {
		t.Fatalf("all private = (%v, %v)", private, set)
	}
	private, _ = Widen([][]string{{"a"}, nil}, []bool{true, false})
	if private {
		t.Fatal("one public member must make the union public")
	}
	private, _ = Widen(nil, nil)
	if private {
		t.Fatal("no members is not private")
	}
}

func TestAdmitsPrincipal(t *testing.T) {
	v := Viewer{Principals: []string{"openwebui:alice"}}
	if !v.AdmitsPrincipal(nil) || !v.AdmitsPrincipal([]string{"openwebui:alice"}) || v.AdmitsPrincipal([]string{"openwebui:bob"}) {
		t.Fatal("AdmitsPrincipal")
	}
	if (Viewer{}).AdmitsPrincipal([]string{"openwebui:alice"}) {
		t.Fatal("a viewer without principals must not match a private resource")
	}
}
