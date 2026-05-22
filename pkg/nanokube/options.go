package nanokube

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	v1 "github.com/cnuss/nanokube/pkg/v1"
)

type OptionsImpl struct {
	cmd *cobra.Command

	name      string
	verbosity int
	dataDir   string

	dirs    sync.Map
	tunnels sync.Map // map[v1.ServiceName]v1.Tunnel
}

var _ v1.Options = &OptionsImpl{}

func NewOptions() v1.Options {
	options := &OptionsImpl{
		name: "nanokube",
		dataDir: func() string {
			cacheDir, _ := os.UserCacheDir()
			os.Setenv("NANOKUBE_HOME", cacheDir)
			return cacheDir
		}(),
		verbosity: 0,
	}
	// dataDir := func() string {
	// 	cacheDir, _ := os.UserCacheDir()
	// 	os.Setenv("NANOKUBE_HOME", cacheDir)
	// 	return cacheDir
	// }()
	// cmd.PersistentFlags().StringVar(&options.name, "name", "nanokube", "cluster name")
	// cmd.PersistentFlags().CountVarP(&options.verbosity, "verbose", "v", "verbosity (-v warn, -vv info, -vvv debug, -vvvv trace)")
	// cmd.PersistentFlags().StringVar(&options.dataDir, "data", dataDir, "data directory")
	return options
}

func (o *OptionsImpl) RunFlags(cmd *cobra.Command) v1.Options {
	o.cmd = cmd
	cmd.Flags().StringVar(&o.dataDir, "data", func() string {
		home, _ := os.UserHomeDir()
		datadir := filepath.Join(home, fmt.Sprintf(".%s", o.Name()))
		os.Setenv("NANOKUBE_HOME", datadir)
		return datadir
	}(), "data directory")
	return o
}

func (o *OptionsImpl) Args() []string {
	args := []string{}
	o.cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if f.Name == "help" {
			return
		}
		if sv, ok := f.Value.(pflag.SliceValue); ok {
			for _, v := range sv.GetSlice() {
				args = append(args, "--"+f.Name+"="+v)
			}
			return
		}
		args = append(args, "--"+f.Name+"="+f.Value.String())
	})
	return args
}

func (o *OptionsImpl) Name() string {
	return o.name
}

func (o *OptionsImpl) Verbosity() int {
	return o.verbosity
}

func (o *OptionsImpl) DataDir() v1.DataDir {
	return v1.DataDir(o.dataDir)
}

func (o *OptionsImpl) DataDirAt(name v1.DataDir) string {
	if dir, ok := o.dirs.Load(name); ok {
		return dir.(string)
	}
	dir := filepath.Join(o.dataDir, string(name))
	os.MkdirAll(dir, 0o755)
	o.dirs.Store(name, dir)
	return dir
}

func (o *OptionsImpl) FilePathAt(file v1.FileName) string {
	path := filepath.Join(o.dataDir, string(file))
	dir := filepath.Dir(path)
	os.MkdirAll(dir, 0o755)
	return path
}

func (o *OptionsImpl) InDataDir(path string) bool {
	clean := filepath.Clean(path) + string(filepath.Separator)
	dir := filepath.Clean(o.dataDir) + string(filepath.Separator)
	return strings.HasPrefix(clean, dir)
}
