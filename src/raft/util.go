/**
 * File   : util.go
 * License: MIT
 * Author : Xinyue Ou <xinyue3ou@gmail.com>
 * Date   : 22.01.2019
 */
package raft

import "log"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
