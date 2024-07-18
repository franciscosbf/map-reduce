package main

import (
	"fmt"
	"os"
	"time"

	"github.com/franciscosbf/map-reduce/internal/mr"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeMaster(os.Args[1:], 10)
	for !m.Done() {
		time.Sleep(time.Second)
	}
}
