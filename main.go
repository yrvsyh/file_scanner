package main

import (
	"flag"
)

var database = flag.String("database", "filelist.db", "file info database path")
var initData = flag.Bool("init", false, "clear data")
var showSql = flag.Bool("sql", false, "show sql")

func main() {

	flag.Parse()
	root := flag.Arg(0)
	if len(root) == 0 {
		root = "."
	}

	fileScanner, err := NewFileScanner(".", *database, *initData, *showSql)
	if err != nil {
		panic(err)
	}

	if err := fileScanner.Scan(); err != nil {
		panic(err)
	}

	if err := fileScanner.UpdateDB(); err != nil {
		panic(err)
	}
}
