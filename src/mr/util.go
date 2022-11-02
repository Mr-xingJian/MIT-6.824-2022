package mr

import (
	"log"
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
