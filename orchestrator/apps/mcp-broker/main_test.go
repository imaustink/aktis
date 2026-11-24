package main

import (
	"strings"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	cases := []struct {
		name    string
		env     map[string]string
		want    config
		wantErr string
	}{
		{
			name: "defaults match the TypeScript broker and the chart",
			env:  map[string]string{"ORCHESTRATOR_TOKEN": "t"},
			want: config{namespace: "default", group: "core.aktis.ai", version: "v1alpha1", port: 8080,
				discoveryInterval: 5 * time.Minute, orchestratorToken: "t"},
		},
		{
			name: "every knob is read",
			env: map[string]string{"ORCHESTRATOR_TOKEN": "t", "NAMESPACE": "mcp", "CRD_GROUP": "g", "CRD_VERSION": "v9",
				"PORT": "9090", "DISCOVERY_INTERVAL_MS": "1500"},
			want: config{namespace: "mcp", group: "g", version: "v9", port: 9090,
				discoveryInterval: 1500 * time.Millisecond, orchestratorToken: "t"},
		},
		{name: "refuses to start without ORCHESTRATOR_TOKEN", env: map[string]string{}, wantErr: "ORCHESTRATOR_TOKEN is required"},
		{name: "refuses a bad PORT", env: map[string]string{"ORCHESTRATOR_TOKEN": "t", "PORT": "http"}, wantErr: "PORT"},
		{name: "refuses a bad interval", env: map[string]string{"ORCHESTRATOR_TOKEN": "t", "DISCOVERY_INTERVAL_MS": "0"},
			wantErr: "DISCOVERY_INTERVAL_MS"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := loadConfig(func(k string) string { return tc.env[k] })
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}
