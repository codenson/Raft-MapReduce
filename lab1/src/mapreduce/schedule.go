
package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
///////////////////////////////////////////////////////////////////////////////////
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)
	completedTasks := make(chan bool)
	for index := 0; index < ntasks; index++ {
		go mr.handleTask(index, phase, nios, completedTasks)
	}
	for i := 0; i < ntasks; i++ {
		<-completedTasks
	}
	debug("Schedule: %v phase done\n", phase)

}

/////////////////////////////////////////////////////////////////////////////

func (mr *Master) handleTask(taskNum int, phase jobPhase, nios int, completedTasks chan bool) {
	for {
		worker := <-mr.registerChannel
		check := call(worker, "Worker.DoTask", DoTaskArgs{
			JobName:       mr.jobName,
			File:          mr.files[taskNum],
			Phase:         phase,
			TaskNumber:    taskNum,
			NumOtherPhase: nios,
		}, new(struct{}))

		if !check {
			fmt.Printf("WARNING: task number %d scheduling failed on worker %s \n", taskNum, worker)
		} else {
			completedTasks <- true
			mr.registerChannel <- worker
			break
		}
	}
}
