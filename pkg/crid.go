package pkg

import (
	"fmt"
	"sync"

	"github.com/cnuss/nanokube/pkg/nanokube"
	v1 "github.com/cnuss/nanokube/pkg/v1"
)

type (
	Runtime    string
	DetectFunc func(config v1.Config) v1.Backend
)

var Runtimes = map[Runtime]DetectFunc{}

type CridImpl struct {
	config v1.Config

	backends     sync.Map // map[string]Backend
	backendsOnce sync.Once
}

var _ v1.Crid = &CridImpl{}

func newCrid(config v1.Config) v1.Crid {
	crid := &CridImpl{
		config:   config,
		backends: sync.Map{},
	}
	return crid
}

func (c *CridImpl) Backends() map[string]v1.Backend {
	c.backendsOnce.Do(func() {
		detected := 0
		for name, detect := range Runtimes {
			backend := detect(c.config)
			if backend != nil {
				nanokube.Log.Info("runtime detected", "runtime", name)
				c.backends.Store(string(name), backend)
				detected++
			}
		}
		if detected == 0 {
			c.config.Cancel(NewFatalError(fmt.Errorf("no container runtime backend available")).WithCode(-1))
		}
	})

	backends := make(map[string]v1.Backend)
	c.backends.Range(func(key, value any) bool {
		name := key.(string)
		backend := value.(v1.Backend)
		backends[name] = backend
		return true
	})
	return backends
}

func (c *CridImpl) DefaultBackend() v1.Backend {
	for _, backend := range c.Backends() {
		return backend
	}
	c.config.Cancel(NewFatalError(fmt.Errorf("no container runtime backend available")).WithCode(-1))
	select {}
}
