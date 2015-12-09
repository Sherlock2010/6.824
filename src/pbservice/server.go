package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

//import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type PBServer struct {
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  done sync.WaitGroup
  finish chan interface{}
  // Your declarations here.
  viewnum uint
  role string
  backup string

  db map[string]string
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  key := args.Key
  value := args.Value
  dohash := args.DoHash
  tag := args.Tag

  if tag == Update {
    // Update

    if pb.backup != "" {
      ok := call(pb.backup, "PBServer.Put", args, &reply)
      if ok == false {
        DPrintf("[INFO] Fail to call %s PBServer.Put ...\n", pb.backup)
      }
      
    }
    
    if !dohash {
      //Put
      _, ok := pb.db[key]
  
      if ok {
        pb.db[key] = value
        
      } else {
        pb.db[key] = value
        
      } 
      DPrintf("[INFO] Update server %s key %s , value %s ...\n", pb.me, key, value)
      reply.Err = OK
      reply.PreviousValue = ""
  
      return nil

    } else{
      // Put Hash

    }
  } else if tag == Init {
    // Init
    pb.db = args.DB
    reply.Err = OK
    DPrintf("[INFO] Primary %s init backup %s ...\n", pb.me, pb.backup)
    
  } else {
    // Over
    if !dohash {
      //Put
      _, ok := pb.db[key]
  
      if ok {
        pb.db[key] = value

      } else {
        pb.db[key] = value
        
      }
      DPrintf("[INFO] Update server %s key %s , value %s ...\n", pb.me, key, value)
      reply.Err = OK
      reply.PreviousValue = ""
  
      return nil

    } else{
      // Put Hash

    }

  }

  return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  key := args.Key

  if pb.dead == false {
    value, ok := pb.db[key]
    if ok {
      reply.Value = value
      reply.Err = OK

    } else {
      reply.Value = ""
      reply.Err = ErrNoKey

    }
  } else {
    reply.Err = ErrWrongServer

  }

  return nil
}


// ping the viewserver periodically.
func (pb *PBServer) tick() {

  View, _ := pb.vs.Ping(pb.viewnum)
  // DPrintf("[INFO] %s %s Ping, viewnum %d ...\n", pb.role, pb.me, pb.viewnum)
  pb.viewnum = View.Viewnum
  // DPrintf("[INFO] View Primary %s, Backup %s ...\n", View.Primary, View.Backup)
  if pb.me == View.Primary {
    //server is primary
    pb.role = Primary

    if pb.backup != View.Backup {    
      pb.backup = View.Backup
      DPrintf("[INFO] Detect backup %s ...\n", pb.backup)
      // Init
      args := &PutArgs{}
      args.DB = pb.db
      args.Tag = Init
      var reply PutReply

      ok := call(pb.backup, "PBServer.Put", args, &reply)
      if ok == false {
        DPrintf("[INFO] Fail to call %s Put ...\n", pb.backup)
      }
      
    }
  } else {
    //server is backup
    pb.role = Backup
    pb.backup = ""
  }

}


// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  DPrintf("[INFO] kill %s ...\n", pb.me)
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.finish = make(chan interface{})
  // Your pb.* initializations here.
  pb.viewnum = 0
  pb.backup = ""
  pb.db = make(map[string]string)

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        } else {
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
    DPrintf("%s: wait until all request are done\n", pb.me)
    pb.done.Wait() 
    // If you have an additional thread in your solution, you could
    // have it read to the finish channel to hear when to terminate.
    close(pb.finish)
  }()

  pb.done.Add(1)
  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
    pb.done.Done()
  }()

  return pb
}
