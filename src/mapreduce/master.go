package mapreduce
import "container/list"
import "fmt"

type WorkerInfo struct {
  address string
  // You can add definitions here.

}




// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) RunMaster() *list.List {

  // run background, when worker finish jod, register its address back to channel
  go mr.DoRegister()

  // sent map job to available worker
  for i := 0; i < nMap; i++ {
    WorkerAddress := <- mr.registerChannel
    arg := &DoJobArgs{}

    arg.File = mr.file
    arg.Operation = Map
    arg.JobNumber  = i       
    arg.NumOtherPhase = nReduce

    var reply DoJobReply
    ok := call(WorkerAddress, "Worker.DoJob", arg, &reply)

    if reply.OK == true {
      mr.WorkerDoneChannel <- WorkerAddress
    }
    
    if ok == false {
      fmt.Printf("Register: RPC %s register error\n", WorkerAddress)
    }

    fmt.Printf("Finish: Worker %s finish map job %d\n", WorkerAddress, i)
  
  }

  // sent reduce job to available worker
  for i := 0; i < nReduce; i++ {
    WorkerAddress := <- mr.registerChannel
    arg := &DoJobArgs{}

    arg.File = mr.file
    arg.Operation = Reduce
    arg.JobNumber  = i       
    arg.NumOtherPhase = nMap

    var reply DoJobReply
    ok := call(WorkerAddress, "Worker.DoJob", arg, &reply)

    if reply.OK == true {
      mr.WorkerDoneChannel <- WorkerAddress
    }

    if ok == false {
      fmt.Printf("Register: RPC %s register error\n", WorkerAddress)
    }

    fmt.Printf("Finish: Worker %s finish reduce job %d\n", WorkerAddress, i)

  }

  return mr.KillWorkers()
}

func (mr *MapReduce) DoRegister() {
  WorkerAddress := <- mr.WorkerDoneChannel
  Register(mr.MasterAddress, WorkerAddress)
}
