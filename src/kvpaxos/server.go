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

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
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

  // process request before
  done.Wait()
  key := args.key
  value := kv.database[key]

  reply.Value = value
  reply.Err = OK
  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  reply.Err = Nil

  seq := px.seq
  op := MakeOp(Put, args)
  px.Start(seq, op)

  to := 10 * time.Millisecond
  for {
    decided, _ := kv.px.Status(seq)
    if decided {
      kv.Process(seq)
      break
    }

    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }

  return nil
}

func (kv *KVPaxos) Process(seq) {
  px := kv.px
  done.Add(1)
  for _, ins := range px.instances {

    if ins.Seq < seq && ins.OK {
      args, err := ins.Value.(PutArgs)
      if err {
        log.Fatal("Process: ", err);
      }

      doHash := args.DoHash
      key := args.key
      value := args.Value
    
      if doHash {
        previousValue := ""
        previousValue, ok := pb.db[key]
            
        if ok == true {
          newValue := hash(previousValue + value)
          kv.database[key] = strconv.Itoa(int(newValue))
        }
    
        reply.PreviousValue = previousValue
    
        reply.Err = OK
    
      } else {
        kv.database[key] = value
    
        reply.Err = OK
      }
    }
  }

  done.Done()

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

  kv := new(KVPaxos)
  kv.me = me

  // Your initialization code here.

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

