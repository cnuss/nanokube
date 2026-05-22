package v1

import (
	corev1 "k8s.io/api/core/v1"
)

type (
	BackendName string
	BackendFunc func(nano Nanokube) Backend
	DataDir     string
	FileName    string
	Path        string
	NetworkType string
	ServiceName string
	TunnelName  string
	Protocol    corev1.Protocol
)
