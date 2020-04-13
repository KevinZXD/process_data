package main

import (
	"fmt"
	"os"
	"process_data/command"
)

var (
	Version = "0.0.1"
)

var usage string = `
Usage: process_data [-v|--version] [-h|--help] <command> [<args>]

Available commands are:

`

func main() {
	os.Exit(core())
}

func core() int {
	args := os.Args[:] //获取系统cmdline
	//args := []string {"process_data","videoChannel", "server" ,"--config=configs/videoChannel.test.toml" }

	cmdLists := command.GetCmds() //从command 包中执行获取全部功能列表map 实例
	resetUsage(cmdLists)          //归集使用方法

	if len(args) < 2 {
		fmt.Println(usage) //使用方法错误打印使用 help info
		return 1
	}

	cmd := args[1] //获取子命令
	switch cmd {
	case "-v", "--version", "version":
		fmt.Printf("process_data %s\n", Version)
		return 0
	case "-h", "--help":
		fmt.Println(usage)
		return 0
	}
	command, ok := cmdLists[cmd] //获取当前cmd实例

	if !ok {
		fmt.Printf("\033[31m\033[01m\033[05mErorr : can not find Commad : %s  \033[0m \n", cmd)
		fmt.Println(usage)
		return 1
	}

	return command.Run()
}

func resetUsage(cmdLists command.CMDMap) {
	for _, c := range cmdLists {
		usage += fmt.Sprintf("	%s		%s\n", c.Name(), c.Synopsis())
	}
}
