package kubernetes

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	apifeatures "k8s.io/apiserver/pkg/features"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
)

type Command struct {
	*cobra.Command
	ctx          v1.Nanokube
	flagSet      *pflag.FlagSet
	flags        map[string]string
	featureGates map[string]bool
	needs        []*Command

	starting chan struct{}
	started  chan struct{}
	stopping chan struct{}
	stopped  chan struct{}
}

func newCommand(ctx v1.Nanokube, cmd *cobra.Command) *Command {
	c := &Command{
		Command: cmd,
		ctx:     ctx,
		flagSet: cmd.Flags(),
		flags:   make(map[string]string),
		featureGates: map[string]bool{
			string(features.KubeletInUserNamespace): true,
			// DEVNOTE: Use Websockets for Attach/Exec/PortForward
			string(features.TranslateStreamCloseWebsocketRequests): false,
			string(features.PortForwardWebsockets):                 false,
			string(features.ExtendWebSocketsToKubelet):             false,
			// DEVNOTE: SSE not supported with Cloudflare Tunnels
			string(apifeatures.WatchList): false,
		},
		needs:    make([]*Command, 0),
		starting: make(chan struct{}),
		started:  make(chan struct{}),
		stopping: make(chan struct{}),
		stopped:  make(chan struct{}),
	}
	c.Command.RunE = c.runE()
	return c
}

func (c *Command) WithNeed(need *Command) *Command {
	c.needs = append(c.needs, need)
	return c
}

func (c *Command) WithFlag(name, value string) *Command {
	c.flags[name] = value
	return c
}

func (c *Command) WithFlagSet(flagSet *pflag.FlagSet) *Command {
	c.flagSet = flagSet
	return c
}

func (c *Command) runE() func(cmd *cobra.Command, args []string) error {
	logger := klog.FromContext(c.ctx)
	settings := make(map[string]string)

	for k, v := range c.featureGates {
		c.flagSet.Set("feature-gates", func() string {
			if v {
				return fmt.Sprintf("%s=true", k)
			}
			return fmt.Sprintf("%s=false", k)
		}())
	}

	for k, v := range c.flags {
		c.flagSet.Set(k, v)
	}

	c.flagSet.VisitAll(func(f *pflag.Flag) {
		if f.Name == "help" {
			return
		}
		settings[f.Name] = f.Value.String()
		c.flagSet.MarkHidden(f.Name)
	})
	keys := slices.Sorted(maps.Keys(settings))

	inner := c.Command.RunE
	return func(cmd *cobra.Command, args []string) error {
		flags := []string{}
		for _, k := range keys {
			setting := settings[k]
			if strings.Contains(setting, " ") {
				setting = fmt.Sprintf("%q", setting)
			}
			if setting == "" {
				setting = "<empty>"
			}
			flags = append(flags, "--"+k+"="+setting)
		}
		command := fmt.Sprintf("%s %s", cmd.Name(), strings.Join(flags, " "))
		logger.Info("running", "command", command)

		if err := inner(cmd, args); err != nil {
			return err
		}
		return nil
	}
}
