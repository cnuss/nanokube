package config

import (
	"fmt"

	"github.com/spf13/cobra"
)

type Options struct {
	Cmd       *cobra.Command
	Name      string
	Verbosity int
	Clean     bool
	DataDir   string
	Kubelet   bool
}

func NewOptions(cmd *cobra.Command) *Options {
	o := &Options{Cmd: cmd}
	cmd.Flags().StringVar(&o.Name, "name", "nanokube", "cluster name")
	cmd.Flags().CountVarP(&o.Verbosity, "verbose", "v", "verbosity (-v debug, -vv trace)")
	cmd.Flags().BoolVar(&o.Clean, "clean", false, "clean data directory before starting")
	cmd.Flags().BoolVar(&o.Kubelet, "kubelet", true, "start the kubelet component")
	cmd.Flags().StringVar(&o.DataDir, "data", "", "data directory (default ~/.{name})")
	return o
}

func (o *Options) Validate() error {
	if o.Name == "" {
		return fmt.Errorf("name cannot be empty")
	}
	return nil
}
