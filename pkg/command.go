package pkg

import (
	"context"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

type Command struct {
	*cobra.Command

	nano   v1.Nanokube
	cancel context.CancelFunc

	byName    map[string]*cobra.Command
	deps      map[*cobra.Command][]*cobra.Command
	inner     map[*cobra.Command]func(*cobra.Command, []string) error
	cancelFor map[*cobra.Command]context.CancelFunc
	stopped   map[*cobra.Command]chan struct{}
}

func NewNanokubeCommand(ctx context.Context) *Command {
	ctx, cancel := context.WithCancel(ctx)
	cmd := &cobra.Command{
		Use:  "nanokube",
		Long: "all-in-one kubernetes binary",
	}
	cmd.SetContext(ctx)

	c := &Command{
		Command:   cmd,
		cancel:    cancel,
		byName:    map[string]*cobra.Command{},
		deps:      map[*cobra.Command][]*cobra.Command{},
		inner:     map[*cobra.Command]func(*cobra.Command, []string) error{},
		cancelFor: map[*cobra.Command]context.CancelFunc{},
		stopped:   map[*cobra.Command]chan struct{}{},
	}

	// no-arg form: launch kubelet + controller + scheduler (apiserver pulled in via deps)
	cmd.RunE = func(*cobra.Command, []string) error {
		return c.launch(
			c.byName["kubelet"],
			c.byName["kube-controller-manager"],
			c.byName["kube-scheduler"],
		)
	}

	return c
}

// dependentsOf returns commands that have `cmd` in their deps. Computed at
// shutdown time (lazy) so AddCommand order doesn't matter.
func (c *Command) dependentsOf(cmd *cobra.Command) []*cobra.Command {
	var out []*cobra.Command
	for other, deps := range c.deps {
		for _, d := range deps {
			if d == cmd {
				out = append(out, other)
				break
			}
		}
	}
	return out
}

func (c *Command) AddCommand(cmds ...*cobra.Command) {
	for _, cmd := range cmds {
		// Each child gets its own ctx derived from the root, plus a `stopped`
		// chan that closes when its RunE returns. The shutdown goroutine
		// below holds the cancel until every dependent has stopped.
		childCtx, childCancel := context.WithCancel(c.Context())
		cmd.SetContext(childCtx)
		c.cancelFor[cmd] = childCancel
		c.stopped[cmd] = make(chan struct{})

		name := cmd.Name()
		c.byName[name] = cmd

		switch name {
		case "kube-controller-manager":
			c.deps[cmd] = []*cobra.Command{c.byName["kube-apiserver"]}
			// TODO hide everything by default and only unhide a curated subset
			cmd.Flags().Lookup("version").Hidden = true
		case "kube-scheduler":
			c.deps[cmd] = []*cobra.Command{c.byName["kube-apiserver"]}
			// TODO hide everything by default and only unhide a curated subset
			cmd.Flags().Lookup("version").Hidden = true
		case "kube-apiserver":
			// TODO hide everything by default and only unhide a curated subset
			cmd.Flags().Lookup("advertise-address").Hidden = true
			cmd.Flags().Lookup("allow-privileged").DefValue = "true"
			cmd.Flags().Lookup("cert-dir").Hidden = true
			cmd.Flags().Lookup("disable-http2-serving").Hidden = true
			cmd.Flags().Lookup("endpoint-reconciler-type").DefValue = "none"
			cmd.Flags().Lookup("external-hostname").Hidden = true // TODO Set at runtime
			cmd.Flags().Lookup("etcd-cafile").Hidden = true
			cmd.Flags().Lookup("etcd-certfile").Hidden = true
			cmd.Flags().Lookup("etcd-compaction-interval").Hidden = true
			cmd.Flags().Lookup("etcd-compaction-interval").DefValue = "0"
			cmd.Flags().Lookup("etcd-count-metric-poll-period").Hidden = true
			cmd.Flags().Lookup("etcd-count-metric-poll-period").DefValue = "0"
			cmd.Flags().Lookup("etcd-db-metric-poll-interval").Hidden = true
			cmd.Flags().Lookup("etcd-db-metric-poll-interval").DefValue = "0"
			cmd.Flags().Lookup("etcd-healthcheck-timeout").Hidden = true
			cmd.Flags().Lookup("etcd-healthcheck-timeout").DefValue = "30s"
			cmd.Flags().Lookup("etcd-keyfile").Hidden = true
			cmd.Flags().Lookup("etcd-prefix").Hidden = true
			cmd.Flags().Lookup("etcd-readycheck-timeout").Hidden = true
			cmd.Flags().Lookup("etcd-readycheck-timeout").DefValue = "30s"
			cmd.Flags().Lookup("etcd-servers").Hidden = true // TODO Set at runtime
			cmd.Flags().Lookup("etcd-servers-overrides").Hidden = true
			cmd.Flags().Lookup("http2-max-streams-per-connection").Hidden = true
			cmd.Flags().Lookup("kubelet-preferred-address-types").DefValue = "ExternalDNS"
			cmd.Flags().Lookup("kubernetes-service-node-port").DefValue = "443"
			cmd.Flags().Lookup("permit-address-sharing").Hidden = true
			cmd.Flags().Lookup("permit-port-sharing").Hidden = true
			cmd.Flags().Lookup("peer-advertise-ip").Hidden = true
			cmd.Flags().Lookup("peer-advertise-port").Hidden = true
			cmd.Flags().Lookup("peer-ca-file").Hidden = true
			cmd.Flags().Lookup("proxy-client-cert-file").Hidden = true
			cmd.Flags().Lookup("proxy-client-key-file").Hidden = true
			cmd.Flags().Lookup("secure-port").Hidden = true // TODO Set at runtime
			cmd.Flags().Lookup("storage-backend").Hidden = true
			cmd.Flags().Lookup("tls-cert-file").Hidden = true // TODO Set at runtime
			cmd.Flags().Lookup("tls-cipher-suites").Hidden = true
			cmd.Flags().Lookup("tls-curve-preferences").Hidden = true
			cmd.Flags().Lookup("tls-min-version").Hidden = true
			cmd.Flags().Lookup("tls-private-key-file").Hidden = true // TODO Set at runtime
			cmd.Flags().Lookup("tls-sni-cert-key").Hidden = true
			cmd.Flags().Lookup("version").Hidden = true
		case "kubelet":
			// TODO hide everything by default and only unhide a curated subset
			cmd.Flags().Lookup("version").Hidden = true
		default:
			// Unknown command — register as-is, no RunE rewrapping, no gating.
			c.Command.AddCommand(cmd)
			continue
		}

		// Stash the upstream RunE wrapped to close stopped when it returns,
		// then replace cmd.RunE with one that launches target + transitive deps.
		upstream := cmd.RunE
		target := cmd
		c.inner[cmd] = func(cmd *cobra.Command, args []string) error {
			defer close(c.stopped[cmd])
			return upstream(cmd, args)
		}
		cmd.RunE = func(*cobra.Command, []string) error {
			return c.launch(target)
		}
		c.Command.AddCommand(cmd)

		// Reverse-dep shutdown: when root ctx fires, wait for every command
		// that depends on me to close `stopped`, then cancel my own ctx.
		go func(cmd *cobra.Command) {
			<-c.Context().Done()
			for _, dep := range c.dependentsOf(cmd) {
				<-c.stopped[dep]
			}
			c.cancelFor[cmd]()
		}(cmd)
	}
}

// launch runs `cmds` plus their transitive deps as concurrent goroutines and
// waits for all of them. Each goroutine calls the command's RunE directly so
// cobra doesn't re-parse os.Args.
func (c *Command) launch(cmds ...*cobra.Command) error {
	seen := map[*cobra.Command]bool{}
	var order []*cobra.Command
	var visit func(*cobra.Command)
	visit = func(cmd *cobra.Command) {
		if cmd == nil || seen[cmd] {
			return
		}
		seen[cmd] = true
		for _, d := range c.deps[cmd] {
			visit(d)
		}
		order = append(order, cmd)
	}
	for _, cmd := range cmds {
		visit(cmd)
	}

	g := new(errgroup.Group)
	for _, cmd := range order {
		cmd := cmd
		g.Go(func() error { return c.inner[cmd](cmd, nil) })
	}
	return g.Wait()
}
