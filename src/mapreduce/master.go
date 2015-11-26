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
    DPrintf("[DoWork] shutdown %s\n", w.address)
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
    fmt.Printf("[Pre] pull from register channel, channel size %d\n", len(mr.registerChannel))
    WorkerAddress := <- mr.registerChannel
    arg := &DoJobArgs{}

    arg.File = mr.file
    arg.Operation = Map
    arg.JobNumber  = i       
    arg.NumOtherPhase = nReduce

    var reply DoJobReply
    ok := call(WorkerAddress, "Worker.DoJob", arg, &reply)

    if reply.OK == true {

      if mr.WorkerDoneChannel != nil {
        fmt.Printf("[Pre] Worker %s pre to channel, channel size %d\n",WorkerAddress, len(mr.WorkerDoneChannel))
       
        mr.WorkerDoneChannel <- WorkerAddress

        fmt.Printf("[Channel] Worker %s push to channel, channel size %d\n", WorkerAddress, len(mr.WorkerDoneChannel))
      } else {
        fmt.Printf("[Error] WorkerDoneChannel is nil\n")
      }
    
    }
    
    if ok == false {
      fmt.Printf("Register: RPC %s register error\n", WorkerAddress)
    }

    fmt.Printf("[Finish] Worker %s finish map job %d\n\n", WorkerAddress, i)
  
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
      fmt.Printf("[Push] Worker %s push to channel\n", WorkerAddress)
    }

    if ok == false {
      fmt.Printf("[Register] RPC %s register error\n", WorkerAddress)
    }

    fmt.Printf("[Finish] Worker %s finish reduce job %d\n\n", WorkerAddress, i)

  }

  return mr.KillWorkers()
}

func (mr *MapReduce) DoRegister() {
  for {
    WorkerAddress := <- mr.WorkerDoneChannel
  // fmt.Printf("[Pull] pull from channel\n")
  Register(mr.MasterAddress, WorkerAddress)
  // fmt.Printf("[Re-Register] worker %s register\n", WorkerAddress)
  }
}
