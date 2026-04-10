package pkg

import (
	"fmt"
	"sync"
)

type (
	Runtime    string
	DetectFunc func(config Config) Backend
)

var Runtimes = map[Runtime]DetectFunc{}

type Crid interface {
	Backends() map[string]Backend
	DefaultBackend() Backend
}

type CridImpl struct {
	config Config

	backends     sync.Map // map[string]Backend
	backendsOnce sync.Once
}

var _ Crid = &CridImpl{}

func newCrid(config Config) Crid {
	crid := &CridImpl{
		config:   config,
		backends: sync.Map{},
	}
	return crid
}

func (c *CridImpl) Backends() map[string]Backend {
	c.backendsOnce.Do(func() {
		for name, detect := range Runtimes {
			if backend := detect(c.config); backend != nil {
				c.backends.Store(string(name), backend)
			}
		}
	})

	backends := make(map[string]Backend)
	c.backends.Range(func(key, value any) bool {
		name := key.(string)
		backend := value.(Backend)
		backends[name] = backend
		return true
	})
	return backends
}

func (c *CridImpl) DefaultBackend() Backend {
	for _, backend := range c.Backends() {
		return backend
	}
	c.config.Cancel(NewFatalError(fmt.Errorf("no container runtime backend available")))
	return nil
}
