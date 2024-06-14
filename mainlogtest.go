package main

import (
	//"log"
	_ "net/http/pprof"
	"os"
	"starlink-world/erigon-evm/params"

	"starlink-world/erigon-evm/log"
)

func main() {
	//std.Output(2, fmt.Sprintf(format, v...))
	//log.Lvl() = log.LvlInfo
	//.SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StdoutHandler))

	logger := log.New()
	logger.SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StdoutHandler))
	//log.Lvl()
	//log.SetFlags(log.Lshortfile | log.Lmicroseconds | log.Ldate)
	//v := "很普通的"
	//log.Printf("这是一条%s日志。\n", v)
	////std.Output(2, fmt.Sprintln(v...))
	//log.Println("这是一条很普通的日志。")
	////std.Output(2, fmt.Sprintln(v...))
	////os.Exit(1)
	//log.Fatalln("这是一条会触发fatal的日志。")
	////s := fmt.Sprintln(v...)
	////std.Output(2, s)
	////panic(s)
	//log.Panicln("这是一条会触发panic的日志。")

	// logger := log.New()
	// initializing the node and providing the current git commit there
	log.Info("Program starting", "args", os.Args)
	logger.Info("Build info", "git_branch", params.GitBranch, "git_tag", params.GitTag, "git_commit", params.GitCommit)
	logger.Info("Program starting", "args", os.Args)
}
