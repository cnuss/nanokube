package backend

import (
	"context"
	"fmt"
	"sync"

	"github.com/cnuss/nanokube/pkg/component"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
)

func NewEventRecorder(backend Backend) *EventRecorderImpl {
	return &EventRecorderImpl{
		log:       component.NewLogger("event-recorder"),
		backend:   backend,
		nodeReady: make(chan struct{}),
	}
}

type EventRecorderImpl struct {
	log          component.Logger
	backend      Backend
	recorder     record.EventRecorder
	recorderOnce sync.Once

	nodeReady     chan struct{}
	nodeReadyOnce sync.Once
}

func (e *EventRecorderImpl) Recorder() record.EventRecorder {
	e.recorderOnce.Do(func() {
		e.recorder = e.backend.EventBroadcaster().NewRecorder(legacyscheme.Scheme, v1.EventSource{Component: string(e.backend.Name()), Host: e.backend.Hosts().Hostname()})
	})
	return e.recorder
}

// AnnotatedEventf implements [record.EventRecorder].
func (e *EventRecorderImpl) AnnotatedEventf(object runtime.Object, annotations map[string]string, eventtype string, reason string, messageFmt string, args ...interface{}) {
	e.log.Info().Str("fn", "AnnotatedEventf").Str("type", eventtype).Str("reason", reason).Msg(fmt.Sprintf(messageFmt, args...))
	e.Recorder().AnnotatedEventf(object, annotations, eventtype, reason, messageFmt, args...)
}

// Event implements [record.EventRecorder].
func (e *EventRecorderImpl) Event(object runtime.Object, eventtype string, reason string, message string) {
	e.log.Info().Str("fn", "Event").Str("type", eventtype).Str("reason", reason).Msg(message)
	e.checkNodeReady(reason)
	e.Recorder().Event(object, eventtype, reason, message)
}

// Eventf implements [record.EventRecorder].
func (e *EventRecorderImpl) Eventf(object runtime.Object, eventtype string, reason string, messageFmt string, args ...interface{}) {
	e.log.Info().Str("fn", "Eventf").Str("type", eventtype).Str("reason", reason).Msg(fmt.Sprintf(messageFmt, args...))
	e.checkNodeReady(reason)
	e.Recorder().Eventf(object, eventtype, reason, messageFmt, args...)
}

func (e *EventRecorderImpl) checkNodeReady(reason string) {
	if reason == "NodeReady" {
		e.nodeReadyOnce.Do(func() {
			e.log.Info().Msg("node is ready")
			close(e.nodeReady)
		})
	}
}

func (e *EventRecorderImpl) WaitForNodeReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.nodeReady:
		return nil
	}
}

var _ record.EventRecorder = &EventRecorderImpl{}
