package kubernetes

import (
	"context"
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
	parent v1.Nanokube

	ctx    context.Context
	cancel context.CancelFunc

	flagSet      *pflag.FlagSet
	flags        map[string]string
	featureGates map[string]bool

	needs      []*Command
	dependents []*Command

	stopped chan struct{}
}

func newCommand(parent v1.Nanokube, cobraCmd *cobra.Command) *Command {
	ctx, cancel := parent.WithCancel()
	c := &Command{
		Command: cobraCmd,
		parent:  parent,
		ctx:     ctx,
		cancel:  cancel,
		flagSet: cobraCmd.Flags(),
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
		needs:   make([]*Command, 0),
		stopped: make(chan struct{}),
	}
	// Inject our per-command ctx so the cobra cmd's RunE picks it up via
	// cmd.Context() (upstream cmds patched to prefer that over their closure ctx).
	cobraCmd.SetContext(ctx)
	c.Command.RunE = c.runE()

	// Shutdown ordering: when the root context fires, wait for every command
	// that depends on me to close `stopped`, then cancel my own ctx so the
	// inner cobra command unwinds.
	go func() {
		<-parent.Done()
		for _, dep := range c.dependents {
			<-dep.stopped
		}
		c.cancel()
	}()

	return c
}

// Context returns this Command's lifecycle ctx — derived from the root
// nanokube but only cancels after all dependents have closed `stopped`.
func (c *Command) Context() context.Context {
	return c.ctx
}

// Stopped returns a channel closed when this Command's RunE has returned.
func (c *Command) Stopped() <-chan struct{} {
	return c.stopped
}

// WithNeed records a forward edge (c needs `need`) and the reverse edge
// (`need` is needed by c). The reverse edge is what drives shutdown order.
func (c *Command) WithNeed(need *Command) *Command {
	c.needs = append(c.needs, need)
	need.dependents = append(need.dependents, c)
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
		defer close(c.stopped)

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
