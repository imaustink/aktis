{{- define "temporal-engine.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "temporal-engine.temporalEnv" -}}
- name: TEMPORAL_ADDRESS
  value: {{ .Values.temporal.address | quote }}
- name: TEMPORAL_NAMESPACE
  value: {{ .Values.temporal.namespace | quote }}
- name: TASK_QUEUE
  value: {{ .Values.taskQueue | quote }}
{{- end }}

{{- define "temporal-engine.qdrantEnv" -}}
- name: QDRANT_HOST
  value: {{ .Values.qdrant.host | quote }}
- name: QDRANT_PORT
  value: {{ .Values.qdrant.port | quote }}
{{- with .Values.qdrant.collectionPrefix }}
- name: QDRANT_COLLECTION_PREFIX
  value: {{ . | quote }}
{{- end }}
{{- end }}

{{- define "temporal-engine.callbackBaseURL" -}}
{{- if .Values.callback.baseURL -}}
{{ .Values.callback.baseURL }}
{{- else -}}
http://{{ .Release.Name }}-temporal-engine-gateway-callback.{{ .Release.Namespace }}.svc:8081
{{- end -}}
{{- end }}

{{/*
Namespace the catalog-sync watches Corpus/KnowledgeBase CRs in. Precedence:
knowledgeBases.namespace, then the umbrella chart's global.knowledgeBaseNamespace,
then the catalog namespace (catalog.namespace, else the release namespace).
Lets the engine read knowledge bases from a namespace of their own.
*/}}
{{- define "temporal-engine.knowledgeBaseNamespace" -}}
{{- $g := .Values.global | default dict -}}
{{- .Values.knowledgeBases.namespace | default $g.knowledgeBaseNamespace | default (.Values.catalog.namespace | default .Release.Namespace) -}}
{{- end -}}

{{/*
OpenTelemetry export (docs/adr/0050): on when the umbrella chart's
global.observability.enabled is, pointing at its collector. Unset, the
engine's tracer is a no-op.
*/}}
{{- define "temporal-engine.otelEnv" -}}
{{- $g := .Values.global | default dict }}
{{- $obs := $g.observability | default dict }}
{{- if $obs.enabled }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ printf "http://%s-otel-collector:4318" .Release.Name | quote }}
- name: OTEL_SERVICE_NAME
  value: {{ .serviceName | quote }}
- name: AGENT_TRACE_CONTENT_CEILING
  value: {{ $obs.contentCapture | default "metadata" | quote }}
{{- end }}
{{- end -}}

{{/*
Image attachments (orchestrator/engines/temporal/internal/attachments): the
whole S3 connection profile comes from one Secret, in the same key shape as
the image-gen tool's (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, IMAGE_S3_BUCKET,
IMAGE_S3_REGION, IMAGE_S3_ENDPOINT, IMAGE_S3_FORCE_PATH_STYLE; optional
ATTACHMENTS_S3_PREFIX), so a deployment can point both at one bucket. The
gateway uploads and the worker reads back and presigns, so both mount it.
*/}}
{{- define "temporal-engine.attachmentsEnvFrom" -}}
{{- if .Values.attachments.s3.secretName }}
envFrom:
  - secretRef:
      name: {{ .Values.attachments.s3.secretName }}
{{- end }}
{{- end -}}
