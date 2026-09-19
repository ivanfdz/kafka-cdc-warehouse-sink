{{/*
Chart name, truncated to the 63 character label limit.
*/}}
{{- define "cdc-connect.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Chart name and version, used for the helm.sh/chart label.
*/}}
{{- define "cdc-connect.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Standard labels applied to every resource in this chart.
*/}}
{{- define "cdc-connect.labels" -}}
app.kubernetes.io/name: {{ include "cdc-connect.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
helm.sh/chart: {{ include "cdc-connect.chart" . }}
{{- end -}}
