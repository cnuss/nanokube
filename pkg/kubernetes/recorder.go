package kubernetes

import (
	"k8s.io/apimachinery/pkg/runtime"
)

var recorderLog = component("recorder")

// EventRecorder implements record.EventRecorder with warn logging.
type EventRecorder struct{}

func (EventRecorder) Event(object runtime.Object, eventtype, reason, message string) {
	recorderLog.Debug().Str("type", eventtype).Str("reason", reason).Str("message", message).Msg("Event not implemented")
}

func (EventRecorder) Eventf(object runtime.Object, eventtype, reason, messageFmt string, args ...interface{}) {
	recorderLog.Debug().Str("type", eventtype).Str("reason", reason).Str("messageFmt", messageFmt).Msg("Eventf not implemented")
}

func (EventRecorder) AnnotatedEventf(object runtime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
	recorderLog.Debug().Str("type", eventtype).Str("reason", reason).Str("messageFmt", messageFmt).Msg("AnnotatedEventf not implemented")
}
