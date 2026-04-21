package docker

import (
	"fmt"
	"net/http"
	"sync"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	"github.com/emicklei/go-restful/v3"
	"github.com/google/uuid"
)

type Streams interface {
	New() Stream
	Service() *restful.WebService
}

type StreamsImpl struct {
	driver  v1.Driver
	streams sync.Map // streamID -> *StreamImpl
}

var _ Streams = &StreamsImpl{}

func NewStreams(driver v1.Driver) Streams {
	return &StreamsImpl{
		driver: driver,
	}
}

func (s *StreamsImpl) Service() *restful.WebService {
	ws := new(restful.WebService)
	ws.Path(s.driver.BaseURL().Path + "/streams")

	handler := func(req *restful.Request, resp *restful.Response) {
		streamID := req.PathParameter("streamID")
		value, ok := s.streams.Load(streamID)
		if !ok {
			resp.WriteError(http.StatusNotFound, fmt.Errorf("stream %s not found", streamID))
			return
		}
		stream, ok := value.(*StreamImpl)
		if !ok {
			resp.WriteError(http.StatusInternalServerError, fmt.Errorf("stream %s has invalid type", streamID))
			return
		}
		stream.ServeHTTP(resp.ResponseWriter, req.Request)
	}

	ws.Route(ws.GET("/{streamID}").To(handler))
	ws.Route(ws.POST("/{streamID}").To(handler))
	return ws
}

func (s *StreamsImpl) New() Stream {
	stream := &StreamImpl{
		id:     uuid.NewString(),
		driver: s.driver,
	}
	s.streams.Store(stream.id, stream)
	return stream
}

type Stream interface {
	ID() string
	URL() string
	WithHandler(handler http.Handler) Stream
}

type StreamImpl struct {
	id      string
	driver  v1.Driver
	handler http.Handler
	once    sync.Once
}

var _ Stream = &StreamImpl{}

func (s *StreamImpl) ID() string {
	return s.id
}

func (s *StreamImpl) WithHandler(handler http.Handler) Stream {
	s.once.Do(func() {
		s.handler = handler
	})
	return s
}

func (s *StreamImpl) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.handler == nil {
		http.Error(w, fmt.Sprintf("stream %s: no handler", s.id), http.StatusInternalServerError)
		return
	}
	s.handler.ServeHTTP(w, r)
}

func (s *StreamImpl) URL() string {
	return fmt.Sprintf("%s/streams/%s", s.driver.BaseURL().String(), s.id)
}
