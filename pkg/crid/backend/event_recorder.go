package backend

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
)

func NewEventRecorder(backend *BackendImpl) record.EventRecorder {
	eventRecorder := &EventRecorderImpl{backend: backend}
	// TODO start
	return eventRecorder
}

type EventRecorderImpl struct {
	backend *BackendImpl
}

// AnnotatedEventf implements [record.EventRecorder].
func (e *EventRecorderImpl) AnnotatedEventf(object runtime.Object, annotations map[string]string, eventtype string, reason string, messageFmt string, args ...interface{}) {
	panic("AnnotatedEventf: unimplemented")
}

// Event implements [record.EventRecorder].
func (e *EventRecorderImpl) Event(object runtime.Object, eventtype string, reason string, message string) {
	panic("Event: unimplemented")
}

// Eventf implements [record.EventRecorder].
func (e *EventRecorderImpl) Eventf(object runtime.Object, eventtype string, reason string, messageFmt string, args ...interface{}) {
	panic("Eventf: unimplemented")
}

var _ record.EventRecorder = &EventRecorderImpl{}
