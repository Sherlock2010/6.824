package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
import "strconv"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    // log.Printf(format, a...)
    n, err = fmt.Printf(format, a...)
  }
  return
}


type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  Type Type
  // PutArgs or GetArgs
  Value interface{} 
}

func (kv *KVPaxos) MakeOp(tp Type, v interface{}) Op {
  // init op
  var op Op
  
  op.Type = tp
  op.Value = v
  
  return op
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
  done sync.WaitGroup
  database map[string]string
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  kv.mu.Lock()
  reply.Err = Nil
  px := kv.px

  seq := px.Seq
  op := kv.MakeOp(Get, *args)
  
  kv.px.Start(seq, op)

  to := 10 * time.Millisecond
  for {
    decided, _ := kv.px.Status(seq)
    if decided {
      // DPrintf("[INFO] Get Paxos seq %d, %v ...\n", seq, *args)
      // process paxos log seq < seq
      kv.Process(seq)

      // process paxos log seq = seq
      
      key := args.Key
      value := kv.database[key]

      reply.Value = value
      reply.Err = OK   

      kv.px.InsMap[seq].Done = true 
      break
    }

    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }
  kv.mu.Unlock()
  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock()
  reply.Err = Nil
  px := kv.px

  seq := px.Seq
  op := kv.MakeOp(Put, *args)
  kv.px.Start(seq, op)
  
  to := 10 * time.Millisecond
  for {
    decided, _ := kv.px.Status(seq)
    if decided {
      key := args.Key
      value := args.Value
      doHash := args.DoHash
      // DPrintf("[INFO] Put Paxos seq %d, %v ...\n", seq, *args)
      if doHash {

        // precess paxos log seq < seq
        kv.Process(seq)

        // precess paxos log seq = seq
        previousValue := ""

        _, ok := kv.database[key]
                      
        if ok == true {
          previousValue = kv.database[key]
        }
          
        newValue := hash(previousValue + value)
        kv.database[key] = strconv.Itoa(int(newValue))

        reply.PreviousValue = previousValue
        reply.Err = OK  

        kv.px.InsMap[seq].Done = true  
      } else {
        reply.PreviousValue = ""
        reply.Err = OK
      }

      break
    }

    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }
  kv.mu.Unlock()
  return nil
}

func (kv *KVPaxos) Process(curseq int) (Type, string, Err) {
  
  N := kv.px.InsMap[curseq].Num
  
  for iter := kv.px.Seqs.Front();iter != nil ;iter = iter.Next() {
    seq := iter.Value.(int)

    ins :=kv.px.InsMap[seq] 
    DPrintf("[INFO] %d Try Prepare Execute Seq %d %v %t, curseq %d ...\n", kv.me, seq, ins.V, ins.Done, curseq)
    if ins.Num < N && ins.OK && !ins.Done{
      DPrintf("[INFO] %d Prepare Execute Seq %d %v %t, curseq %d ...\n", kv.me, seq, ins.V, ins.Done, curseq)
      // DPrintf("[INFO] %d Prepare Execute Seq %d %t, curseq %d ...\n", kv.me, seq, ins.Done, curseq)
      op, ok := ins.V.(Op)

      if ok == false {
        DPrintf("Type Err ...\n")
      } 
  
      switch op.Type {
        
        case Put : {
          args, ok := op.Value.(PutArgs)
    
          if ok == false {
            log.Fatal("Type Err")
          } 
              
          doHash := args.DoHash
          key := args.Key
          value := args.Value
          
          if doHash {
        
            previousValue := ""
            _, ok := kv.database[key]
                      
            if ok == true {
              previousValue = kv.database[key]
            }
          
            newValue := hash(previousValue + value)
            kv.database[key] = strconv.Itoa(int(newValue))
            ins.Done = true
          
            DPrintf("[INFO] %d PutHash, Seq %d, key:%s, value:%s ...\n", kv.me, ins.Seq, key, value)
            DPrintf("[INFO] %d PutHash, Seq %d, previousValue:%v, newValue:%v ...\n ", kv.me, ins.Seq, previousValue, newValue)
            // return Put, previousValue, OK
          } else {
            kv.database[key] = value
            ins.Done = true
            DPrintf("[INFO] %d Put, Seq %d, key:%s, value:%s ...\n", kv.me, ins.Seq, key, kv.database[key])
            // DPrintf("[INFO] %d Put, Seq %d, key:%s, value:%d ...\n", kv.me, ins.Seq, key, len(kv.database[key]))

          }
    
        }
        case Get : {
          args, ok := op.Value.(GetArgs)

          if ok == false {
            DPrintf("Type Err ...\n")
          } 
              
          key := args.Key
          DPrintf("[INFO] %d Get, Seq %d, key:%s, value:%s ...\n", kv.me, ins.Seq, key, kv.database[key])
          // return Get, kv.database[key], OK
        }
      }

      kv.px.Done(ins.Seq)
    }
  }

  kv.px.Min()

  return Nil, "", Nil
}


// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *KVPaxos {
  // call gob.Register on structures you want
  // Go's RPC library to marshall/unmarshall.
  gob.Register(Op{})
  gob.Register(PutArgs{})
  gob.Register(GetArgs{})
  gob.Register(PutReply{})
  gob.Register(GetReply{})

  kv := new(KVPaxos)
  kv.me = me

  // Your initialization code here.
  kv.database = make(map[string]string)

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l


  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && kv.dead == false {
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}

