{{- define "connection-broker.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "connection-broker.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "connection-broker.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "connection-broker.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "connection-broker.selectorLabels" -}}
app.kubernetes.io/name: {{ include "connection-broker.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "connection-broker.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "connection-broker.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Namespace the broker reads Corpus/Connection CRs and their credential Secrets
from. Precedence: this subchart's knowledgeBaseNamespace, then the umbrella
chart's global.knowledgeBaseNamespace, then the release namespace. Lets the
broker be pointed at a dedicated KB namespace separate from where the release
is installed, without ever reading across namespaces.
*/}}
{{- define "connection-broker.knowledgeBaseNamespace" -}}
{{- $g := .Values.global | default dict -}}
{{- .Values.knowledgeBaseNamespace | default $g.knowledgeBaseNamespace | default .Release.Namespace -}}
{{- end -}}

{{/*
The indexer (docs/adr/0059): corpus-sync, a separate Deployment from the
broker. Its selector labels differ from the broker's in `name`, so the
broker's Service never routes to it.
*/}}
{{- define "connection-broker.syncFullname" -}}
{{- printf "%s-sync" (include "connection-broker.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "connection-broker.syncServiceAccountName" -}}
{{- if .Values.sync.serviceAccount.create -}}
{{- default (include "connection-broker.syncFullname" .) .Values.sync.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.sync.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "connection-broker.syncSelectorLabels" -}}
app.kubernetes.io/name: {{ printf "%s-sync" (include "connection-broker.name" .) }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "connection-broker.syncLabels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "connection-broker.syncSelectorLabels" . }}
app.kubernetes.io/component: corpus-sync
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Whether this release indexes: Qdrant, an embeddings key and Temporal are all
configured. Otherwise the broker serves its API only -- a real arrangement
(another process may own ingestion) -- and no indexer is deployed. Renders
"true" or nothing.
*/}}
{{- define "connection-broker.syncEnabled" -}}
{{- if and .Values.qdrant.url .Values.embeddings.apiKeySecret.name .Values.temporal.address -}}
true
{{- end -}}
{{- end -}}
