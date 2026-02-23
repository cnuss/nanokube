package internal

import (
	pkgconfig "github.com/cnuss/nanokube/pkg/config"
	nanokubelet "github.com/cnuss/nanokube/pkg/kubelet"
)

type Kubelet struct {
	config *pkgconfig.Config
	nk     *nanokubelet.NanoKubelet
}

func NewKubelet(config *pkgconfig.Config) *Kubelet {
	return &Kubelet{
		config: config,
		nk:     nanokubelet.New(config),
	}
}

func (k *Kubelet) Start() error {
	return k.nk.Start(k.config.Ctx)
}
