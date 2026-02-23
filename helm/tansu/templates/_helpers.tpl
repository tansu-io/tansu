{{/*
Expand the name of the chart.
*/}}
{{- define "tansu.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "tansu.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "tansu.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "tansu.labels" -}}
helm.sh/chart: {{ include "tansu.chart" . }}
{{ include "tansu.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "tansu.selectorLabels" -}}
app.kubernetes.io/name: {{ include "tansu.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Storage engine URL from values (when not using storageUrlFromSecret).
For postgres with password from secret, use storageUrlFromSecret with a secret containing the full URL.
*/}}
{{- define "tansu.storageEngineUrl" -}}
{{- $type := .Values.storageEngine.type -}}
{{- if eq $type "memory" -}}
memory://{{ .Values.storageEngine.memory.prefix }}/
{{- else if eq $type "s3" -}}
s3://{{ .Values.storageEngine.s3.bucket }}/{{ .Values.storageEngine.s3.path | trimSuffix "/" }}{{- if .Values.storageEngine.s3.path }}/{{ end }}
{{- else if eq $type "postgres" -}}
postgres://{{ .Values.storageEngine.postgres.user }}:{{ .Values.storageEngine.postgres.password | default "" }}@{{ .Values.storageEngine.postgres.host }}:{{ .Values.storageEngine.postgres.port }}/{{ .Values.storageEngine.postgres.database }}
{{- else if eq $type "sqlite" -}}
sqlite://{{ .Values.storageEngine.sqlite.path }}
{{- else -}}
memory://tansu/
{{- end -}}
{{- end }}

{{/*
Whether storage URL comes from a secret (key STORAGE_ENGINE).
*/}}
{{- define "tansu.storageUrlFromSecret" -}}
{{- if and .Values.storageUrlFromSecret .Values.storageUrlFromSecret.name .Values.storageUrlFromSecret.key }}true{{ end }}
{{- end }}

{{/*
ServiceAccount name.
*/}}
{{- define "tansu.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "tansu.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
