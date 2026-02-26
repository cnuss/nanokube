package config

import (
	"fmt"
)

type Options struct {
	Name      string
	Verbosity int
	Clean     bool
	DataDir   string
	Kubelet   bool
}

func NewOptions() *Options {
	return &Options{
		Name:      "nanokube",
		Verbosity: 0,
		Clean:     false,
		DataDir:   "",
		Kubelet:   true,
	}
}

func (o *Options) Validate() error {
	if o.Name == "" {
		return fmt.Errorf("name cannot be empty")
	}
	if o.DataDir == "" {
		return fmt.Errorf("data-dir cannot be empty")
	}
	return nil
}
