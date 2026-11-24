package resource

import (
	"regexp"
	"strings"
	"testing"
)

func ptr[T any](v T) *T { return &v }

func server(exposure ...MCPToolExposure) *MCPServer {
	return &MCPServer{
		Metadata: ObjectMeta{Name: "github-mcp", UID: "u-1"},
		Spec:     MCPServerSpec{Transport: "streamable-http", URL: "https://mcp.example/", Exposure: exposure},
	}
}

func TestIsExposed(t *testing.T) {
	// The CRD marks expose with +kubebuilder:default=true: an entry that merely
	// names a tool exposes it.
	cases := []struct {
		name   string
		expose *bool
		want   bool
	}{
		{"defaults to true", nil, true},
		{"explicit true", ptr(true), true},
		{"explicit false", ptr(false), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := IsExposed(MCPToolExposure{RemoteToolName: "t", AllowedRoles: []string{"r"}, Expose: tc.expose})
			if got != tc.want {
				t.Fatalf("IsExposed = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestExposureByRemoteName(t *testing.T) {
	t.Run("keys entries by remote tool name", func(t *testing.T) {
		m := ExposureByRemoteName(server(
			MCPToolExposure{RemoteToolName: "search_issues", AllowedRoles: []string{"eng"}},
			MCPToolExposure{RemoteToolName: "create_issue", AllowedRoles: []string{"eng"}, Expose: ptr(false)},
		))
		if got := strings.Join(m.Keys(), ","); got != "search_issues,create_issue" {
			t.Fatalf("keys = %s", got)
		}
		entry, _ := m.Get("create_issue")
		if entry.Expose == nil || *entry.Expose {
			t.Fatalf("create_issue expose = %v, want false", entry.Expose)
		}
	})

	t.Run("is empty for a server that exposes nothing", func(t *testing.T) {
		if n := ExposureByRemoteName(server()).Len(); n != 0 {
			t.Fatalf("len = %d", n)
		}
	})

	t.Run("last entry wins, first position kept", func(t *testing.T) {
		m := ExposureByRemoteName(server(
			MCPToolExposure{RemoteToolName: "a", AllowedRoles: []string{"one"}},
			MCPToolExposure{RemoteToolName: "b"},
			MCPToolExposure{RemoteToolName: "a", AllowedRoles: []string{"two"}},
			MCPToolExposure{RemoteToolName: ""},
		))
		if got := strings.Join(m.Keys(), ","); got != "a,b" {
			t.Fatalf("keys = %s", got)
		}
		if entry, _ := m.Get("a"); entry.AllowedRoles[0] != "two" {
			t.Fatalf("a = %+v", entry)
		}
	})
}

var hashSuffix = regexp.MustCompile(`-[0-9a-f]{8}$`)

func TestCatalogIDFor(t *testing.T) {
	// Expected values were produced by the TypeScript broker this replaces, so
	// ids already materialized in clusters keep their names across the port.
	cases := []struct {
		name   string
		server string
		entry  MCPToolExposure
		want   string
	}{
		{"prefers an operator-set toolID", "github-mcp",
			MCPToolExposure{RemoteToolName: "search_issues", ToolID: "gh-search"}, "gh-search"},
		{"trims an operator-set toolID", "github-mcp",
			MCPToolExposure{RemoteToolName: "search_issues", ToolID: "  gh-search "}, "gh-search"},
		{"ignores a blank toolID", "srv",
			MCPToolExposure{RemoteToolName: "get-user", ToolID: "   "}, "mcp-srv-get-user"},
		{"derives a readable id for a clean remote tool name (no hash)", "github-mcp",
			MCPToolExposure{RemoteToolName: "search-issues"}, "mcp-github-mcp-search-issues"},
		{"hashes a lossy name (TS parity)", "github-mcp",
			MCPToolExposure{RemoteToolName: "search_issues"}, "mcp-github-mcp-search-issues-18b1e8e0"},
		{"hashes a lossy name (TS parity, 2)", "github-mcp",
			MCPToolExposure{RemoteToolName: "secret_op"}, "mcp-github-mcp-secret-op-ef932ce4"},
		{"underscore", "srv", MCPToolExposure{RemoteToolName: "get_user"}, "mcp-srv-get-user-a4606e02"},
		{"camel case", "srv", MCPToolExposure{RemoteToolName: "getUser"}, "mcp-srv-getuser-abbd3e69"},
		{"exotic", "srv", MCPToolExposure{RemoteToolName: "Weird Tool!"}, "mcp-srv-weird-tool-7ff8f116"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := CatalogIDFor(tc.server, tc.entry); got != tc.want {
				t.Fatalf("CatalogIDFor = %q, want %q", got, tc.want)
			}
		})
	}

	t.Run("gives distinct ids to names that only sanitize to the same string", func(t *testing.T) {
		// The collision guard: `get_user` and `get-user` both sanitize to
		// `...-get-user`. Without a hash on the lossy one, the second MCPTool
		// 409s into a swallowed error and silently never materializes.
		underscore := CatalogIDFor("srv", MCPToolExposure{RemoteToolName: "get_user"})
		hyphen := CatalogIDFor("srv", MCPToolExposure{RemoteToolName: "get-user"})
		camel := CatalogIDFor("srv", MCPToolExposure{RemoteToolName: "getUser"})
		if hyphen != "mcp-srv-get-user" {
			t.Fatalf("hyphen = %q", hyphen)
		}
		if !regexp.MustCompile(`^mcp-srv-get-user-[0-9a-f]{8}$`).MatchString(underscore) {
			t.Fatalf("underscore = %q", underscore)
		}
		if underscore == hyphen || underscore == camel || hyphen == camel {
			t.Fatalf("ids collide: %q %q %q", underscore, hyphen, camel)
		}
	})

	t.Run("is stable across calls for the same inputs", func(t *testing.T) {
		entry := MCPToolExposure{RemoteToolName: "Weird Tool!"}
		if CatalogIDFor("srv", entry) != CatalogIDFor("srv", entry) {
			t.Fatal("not stable")
		}
	})

	t.Run("a lossy over-long name stays legal and keeps the TS suffix", func(t *testing.T) {
		for _, tc := range []struct{ remote, suffix string }{
			{strings.Repeat("a", 260), "-c64163f0"},
			{strings.Repeat("x_", 130), "-166a93d0"},
		} {
			got := CatalogIDFor("s", MCPToolExposure{RemoteToolName: tc.remote})
			if len(got) > maxK8sName || !strings.HasSuffix(got, tc.suffix) || strings.Contains(got, "--") {
				t.Fatalf("CatalogIDFor(%d chars) = %q", len(tc.remote), got)
			}
		}
	})
}

func TestSanitizeName(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{"lowercases and replaces illegal characters", "Search_Issues", "search-issues"},
		{"replaces punctuation", "a/b:c", "a-b-c"},
		{"trims and collapses separators", "--Foo__Bar--", "foo-bar"},
		{"stays readable for an ordinary name (no hash appended)", "mcp-github-search-issues", "mcp-github-search-issues"},
		{"hashes an empty result (TS parity)", "日本語", "mcp-c12140a0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := SanitizeName(tc.in); got != tc.want {
				t.Fatalf("SanitizeName(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}

	t.Run("hashes when the result would be too long", func(t *testing.T) {
		long := SanitizeName(strings.Repeat("x", 300))
		if len(long) > maxK8sName || !hashSuffix.MatchString(long) {
			t.Fatalf("long = %q (%d)", long, len(long))
		}
		if !strings.HasSuffix(long, "-02fd6825") {
			t.Fatalf("long suffix differs from the TS broker: %q", long[len(long)-9:])
		}
		// Distinct over-long inputs keep distinct ids (the hash is of the original).
		other := SanitizeName(strings.Repeat("y", 300))
		if other == long || !strings.HasSuffix(other, "-8214d8f1") {
			t.Fatalf("y = %q", other[len(other)-9:])
		}
	})
}
