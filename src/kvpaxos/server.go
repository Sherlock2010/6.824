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

const Debug = 1

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

  reply.Err = Nil
  px := kv.px

  seq := px.Seq
  op := kv.MakeOp(Get, *args)
  kv.px.Start(seq, op)

  to := 10 * time.Millisecond
  for {
    decided, _ := kv.px.Status(seq)
    if decided {
      getReply := kv.Process(seq)
      reply = getReply.(*GetReply)
      break
    }

    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }

  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  reply.Err = Nil
  px := kv.px

  seq := px.Seq
  op := kv.MakeOp(Put, *args)
  kv.px.Start(seq, op)

  to := 10 * time.Millisecond
  for {
    decided, _ := kv.px.Status(seq)
    if decided {
      putReply := kv.Process(seq)
      reply = putReply.(*PutReply)
      break
    }

    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }

  return nil
}

func (kv *KVPaxos) Process(seq int) interface{} {
 
  ins, ok := kv.px.InsMap[seq]
  if ok == false {
      log.Fatal("Key Err: Seq ", seq);
  }
  
  op, ok := ins.V.(Op)     
  if ok == false {
    log.Fatal("Type Err")
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
    
      DPrintf("[INFO] %d Process Key:%s, Value:%s ...\n", kv.me, key, value)
      reply := &PutReply{}
      if doHash {
    
        previousValue := ""
        _, ok := kv.database[key]
                  
        if ok == true {
          previousValue = kv.database[key]
        }
      
        newValue := hash(previousValue + value)
        kv.database[key] = strconv.Itoa(int(newValue))
      
        reply.PreviousValue = previousValue
        reply.Err = OK
      
        DPrintf("[INFO] %d PutHash, key:%s, value:%s ...\n", kv.me, key, value)
        DPrintf("[INFO] %d PutHash, previousValue:%v, newValue:%v ...\n ", kv.me, previousValue, newValue)
        
      } else {
        kv.database[key] = value
        DPrintf("[INFO] %d Put, key:%s, value:%s ...\n", kv.me, key, kv.database[key])
        reply.Err = OK
      }

      return reply
    }
    case Get : {
      args, ok := op.Value.(GetArgs)
      reply := &GetReply{}
      if ok == false {
        log.Fatal("Type Err")
      } 
          
      key := args.Key

      reply.Value = kv.database[key]
      reply.Err = OK

      return reply
    }
  }

  return nil
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

