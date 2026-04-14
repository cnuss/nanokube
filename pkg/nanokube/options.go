package nanokube

import (
	"fmt"
	"os"
	"path/filepath"
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

func (o *OptionsImpl) DataDir() string {
	return o.dataDir
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

func (o *OptionsImpl) FilePathAt(dir DataDir, path FileName) string {
	return filepath.Join(o.DataDirAt(dir), string(path))
}

func (o *OptionsImpl) FileAt(dir DataDir, name FileNameFn) (string, []byte) {
	filename := name(o)
	path := o.FilePathAt(dir, FileName(filename))
	data, err := os.ReadFile(path)
	if err != nil {
		return path, nil
	}
	return path, data
}
