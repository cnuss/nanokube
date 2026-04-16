package nanokube

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/spf13/cobra"
)

type OptionsImpl struct {
	name       string
	verbosity  int
	clean      bool
	dataDir    string
	standalone bool

	dirs sync.Map
}

var _ Options = &OptionsImpl{}

func NewOptions(cmd *cobra.Command) Options {
	options := &OptionsImpl{}
	cmd.Flags().StringVar(&options.name, "name", "nanokube", "cluster name")
	cmd.Flags().CountVarP(&options.verbosity, "verbose", "v", "verbosity (-v warn, -vv info, -vvv debug, -vvvv trace)")
	cmd.Flags().BoolVar(&options.clean, "clean", false, "clean data directory before starting")
	cmd.Flags().StringVar(&options.dataDir, "data", func() string {
		home, _ := os.UserHomeDir()
		return filepath.Join(home, fmt.Sprintf(".%s", options.Name()))
	}(), "data directory")
	cmd.Flags().BoolVar(&options.standalone, "standalone", false, "run in standalone mode")

	os.Setenv("NANOKUBE_HOME", options.dataDir)
	return options
}

func (o *OptionsImpl) Name() string {
	return o.name
}

func (o *OptionsImpl) Verbosity() int {
	return o.verbosity
}

func (o *OptionsImpl) Clean() bool {
	return o.clean
}

func (o *OptionsImpl) DataDir() DataDir {
	return DataDir(o.dataDir)
}

func (o *OptionsImpl) Standalone() bool {
	return o.standalone
}

func (o *OptionsImpl) DataDirAt(name DataDir) string {
	if dir, ok := o.dirs.Load(name); ok {
		return dir.(string)
	}
	dir := filepath.Join(o.dataDir, string(name))
	os.MkdirAll(dir, 0o755)
	o.dirs.Store(name, dir)
	return dir
}

func (o *OptionsImpl) InDataDir(path string) bool {
	clean := filepath.Clean(path) + string(filepath.Separator)
	dir := filepath.Clean(o.dataDir) + string(filepath.Separator)
	return strings.HasPrefix(clean, dir)
}
