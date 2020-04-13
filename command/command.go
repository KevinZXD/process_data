package command

import (
	"process_data/command/Control"
)

type Command interface {
	Run() int
	Name() string
	Synopsis() string
}

type CMDMap map[string]Command

var cmdMap = CMDMap{}

func init() {
	register(Control.New())

}

func register(c Command) {
	cmdMap[c.Name()] = c
}

func GetCmds() CMDMap {
	return cmdMap
}
