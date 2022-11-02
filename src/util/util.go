package util

import "log"

// Debug
const int Debug = 1

func DPrintf(fmt string, args ...interface{}) (n int, err error) {
    if Debug > 0 {
        log.Printf(fmt, args...)
    }
    return
}
