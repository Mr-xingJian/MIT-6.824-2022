package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = false

// const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func DPrintln(args ...interface{}) {
	if Debug {
		log.Println(args...)
	}
	return
}

func GetNowMillSecond() int64 {
	return time.Now().UnixNano() / 1e6
}
