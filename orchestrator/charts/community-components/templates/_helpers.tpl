{{/*
Common labels for every catalog CR.
*/}}
{{- define "tools.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: controller-agent
{{- end }}

{{/*
GitHub-credential wiring shared by the SWE agents (claudeCodeSweAgent,
opencodeSweAgent), which take the same three mutually-exclusive credentials.
Pass the agent's own values sub-map, e.g.:
  include "tools.githubAppFullyConfigured" .Values.claudeCodeSweAgent

`tools.githubAppFullyConfigured` is non-empty only when ALL THREE
githubApp*SecretKey values are set. `resolveGithubToken`
(framework/github-app-auth) mints an installation token whenever all three
GITHUB_APP_* env vars are present and never looks at GITHUB_TOKEN in that
case, so the PAT secretEnv is dropped rather than forcing a placeholder key
into the Secret just to satisfy the secretRef. A PARTIAL set is deliberately
not treated as configured -- the agent rejects it at runtime, and each
template fails the render for it up front.
*/}}
{{- define "tools.githubAppFullyConfigured" -}}
{{- if and .githubAppIdSecretKey .githubAppPrivateKeySecretKey .githubAppInstallationIdSecretKey -}}
true
{{- end -}}
{{- end }}

{{/*
Count of githubApp*SecretKey values set, as a string -- used to tell "none"
(the PAT path) from a partial set (an error).
*/}}
{{- define "tools.githubAppKeyCount" -}}
{{- len (compact (list .githubAppIdSecretKey .githubAppPrivateKeySecretKey .githubAppInstallationIdSecretKey)) -}}
{{- end }}

{{/*
Claude Code skills vendored under skills/claude-code/<name>/ (docs/adr/0051).
"claudeCodeSkills.files" takes (dict "Files" $.Files "name" <skill>) and
returns a JSON array of {key, path, content}, one per file in the skill's
directory, nested ones included. ConfigMap keys can't contain "/", so the key
encodes the relative path with "__" in its place; the Agent's spec.skills
files list maps each key back. Fails the render on an invalid or unknown
skill name, so a typo can't ship an agent silently missing a skill.
*/}}
{{- define "claudeCodeSkills.files" -}}
{{- $name := .name -}}
{{- if not (regexMatch "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$" $name) -}}
{{- fail (printf "claudeCodeSweAgent.skills: %q is not a valid skill name (lowercase letters, digits and dashes)" $name) -}}
{{- end -}}
{{- $prefix := printf "skills/claude-code/%s/" $name -}}
{{- if not (.Files.Get (printf "%sSKILL.md" $prefix)) -}}
{{- fail (printf "claudeCodeSweAgent.skills: no vendored skill %q (expected %sSKILL.md in the community-components chart)" $name $prefix) -}}
{{- end -}}
{{- $files := list -}}
{{- range $path, $_ := .Files.Glob (printf "%s**" $prefix) -}}
{{- $rel := trimPrefix $prefix $path -}}
{{- $files = append $files (dict "key" (replace "/" "__" $rel) "path" $rel "content" ($.Files.Get $path)) -}}
{{- end -}}
{{- toJson $files -}}
{{- end }}

{{- define "claudeCodeSkills.configMapName" -}}
claude-code-skill-{{ . }}
{{- end }}
