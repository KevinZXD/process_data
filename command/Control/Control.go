package Control

import (
	"fmt"
	"os"

	"github.com/docopt/docopt-go"

	"process_data/Control"
	"process_data/lib/fs"
)

var usage = `
Usage:
	process_data Control  server  --config=<x.toml> [--test]
`

var (
	name     = "Control"
	synopsis = "..."
)

type cmd struct {
	conf struct {
		IsCmdFreqControl bool   `docopt:"Control"`
		IsSubCmdServer   bool   `docopt:"server"`
		CfgFname         string `docopt:"--config"`
		IsTest           bool   `docopt:"--test"`
	}
}

func New() *cmd {
	return &cmd{}
}

func (c *cmd) subCmdServer() int {
	cl := Control.New(c.conf.CfgFname)
	if c.conf.IsTest {
		if err := cl.Server(true); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		return 0
	}
	if err := cl.Server(false); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

func (c *cmd) Run() int {

	opts, err := docopt.ParseDoc(usage)
	if err != nil {
		fmt.Println(usage)
		return 1
	}
	if err := opts.Bind(&c.conf); err != nil {
		fmt.Println(err)
		return 1
	}

	if len(c.conf.CfgFname) != 0 {
		if !fs.IsReadableFile(c.conf.CfgFname) {
			fmt.Errorf("config \"%s\" is not readble", c.conf.CfgFname)
			return 1
		}
	}

	if c.conf.IsSubCmdServer {
		return c.subCmdServer()
	}

	return 0
}

func (c *cmd) Name() string {
	return name
}

func (c *cmd) Synopsis() string {
	return synopsis
}

func (c *cmd) String() string {
	return name
}
