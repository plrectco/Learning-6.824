/**
 * File   : schedule.go
 * License: MIT
 * Author : Xinyue Ou <xinyue3ou@gmail.com>
 * Date   : 13.01.2019
 */
package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	var wg sync.WaitGroup
	wg.Add(ntasks)

	for i := 0; i < ntasks; i++ {
		var file string
		switch phase {
		case mapPhase:
			file = mapFiles[i]
		case reducePhase:
			file = ""
		}
		// Create ntasks routine to run, each one has its own task no
		// use ownI here to avoid closuring the shared varaible i.
		ownI := i
		// Declaration for recursive closure
		var goTask func()
		goTask = func() {
			w := <-registerChan
			ok := call(w, "Worker.DoTask", DoTaskArgs{JobName: jobName, File: file, Phase: phase, TaskNumber: ownI, NumOtherPhase: n_other}, nil)
			fmt.Println(ownI, "job done")
			if !ok {
				// retry
				goTask()
			} else {
				wg.Done()
			}
			registerChan <- w
		}
		// function is not fully defined before they run
		// That is, the varaible inside can be changed
		go goTask()
	}
	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)

}
