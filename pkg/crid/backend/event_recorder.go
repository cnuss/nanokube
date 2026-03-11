package backend

import (
	"fmt"

	"github.com/cnuss/nanokube/pkg/component"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	kubelettypes "k8s.io/kubelet/pkg/types"
)

func NewEventRecorder(backend *BackendImpl) record.EventRecorder {
	log := component.NewLogger("event-recorder")

	var recorder record.EventRecorder
	if host, err := backend.HostInfo(); err == nil && host.Hostname != "" {
		recorder = backend.broadcaster.NewRecorder(
			legacyscheme.Scheme,
			v1.EventSource{Component: string(backend.Name()), Host: host.Hostname},
		)
	} else {
		log.Warn().Err(err).Msg("host info unavailable, using fake event recorder")
		recorder = record.NewFakeRecorder(100)
	}

	impl := &EventRecorderImpl{log: log, backend: backend, recorder: recorder}

	events := backend.Subscribe()
	go func() {
		for {
			select {
			case <-backend.ctx.Done():
				return
			case ev, ok := <-events:
				if !ok {
					return
				}
				impl.handleEvent(ev)
			}
		}
	}()

	return impl
}

type EventRecorderImpl struct {
	log      component.Logger
	backend  *BackendImpl
	recorder record.EventRecorder
}

// AnnotatedEventf implements [record.EventRecorder].
func (e *EventRecorderImpl) AnnotatedEventf(object runtime.Object, annotations map[string]string, eventtype string, reason string, messageFmt string, args ...interface{}) {
	e.log.Info().Str("type", eventtype).Str("reason", reason).Msg(fmt.Sprintf(messageFmt, args...))
	e.recorder.AnnotatedEventf(object, annotations, eventtype, reason, messageFmt, args...)
}

// Event implements [record.EventRecorder].
func (e *EventRecorderImpl) Event(object runtime.Object, eventtype string, reason string, message string) {
	e.log.Info().Str("type", eventtype).Str("reason", reason).Msg(message)
	e.recorder.Event(object, eventtype, reason, message)
}

// Eventf implements [record.EventRecorder].
func (e *EventRecorderImpl) Eventf(object runtime.Object, eventtype string, reason string, messageFmt string, args ...interface{}) {
	e.log.Info().Str("type", eventtype).Str("reason", reason).Msg(fmt.Sprintf(messageFmt, args...))
	e.recorder.Eventf(object, eventtype, reason, messageFmt, args...)
}

func (e *EventRecorderImpl) handleEvent(ev Event) {
	if ev.Resource != ResourceContainer {
		return
	}

	var reason, message string
	switch ev.Action {
	case ActionCreate:
		reason = "Created"
		message = "Created container"
	case ActionStart:
		reason = "Started"
		message = "Started container"
	case ActionStop, ActionKill, ActionDie:
		reason = "Killing"
		message = "Stopping container"
	case ActionOOM:
		reason = "OOMKilled"
		message = "Container was OOM killed"
	default:
		return
	}

	name := kubelettypes.GetPodName(ev.Attributes)
	namespace := kubelettypes.GetPodNamespace(ev.Attributes)
	uid := kubelettypes.GetPodUID(ev.Attributes)
	containerName := kubelettypes.GetContainerName(ev.Attributes)

	if name == "" || namespace == "" || uid == "" {
		return
	}

	if containerName != "" {
		message = fmt.Sprintf("%s %s", message, containerName)
	}

	ref := &v1.ObjectReference{
		Kind:      "Pod",
		Name:      name,
		Namespace: namespace,
		UID:       types.UID(uid),
	}
	e.recorder.Event(ref, v1.EventTypeNormal, reason, message)
}

var _ record.EventRecorder = &EventRecorderImpl{}
