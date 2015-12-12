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

import "strconv"

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
  commitValue string
  previousValue string
  init bool
  commit sync.WaitGroup

  db map[string]string
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  key := args.Key
  value := args.Value
  dohash := args.DoHash
  tag := args.Tag

  if tag == Update {
    // Update

    if !dohash {
      //Put
      if pb.backup != "" {
        putArgs := &PutArgs{}

        putArgs.Key = args.Key
        putArgs.Value = args.Value
        putArgs.DoHash = args.DoHash
        putArgs.Tag = Over

        var putReply PutReply
        putReply.Err = Nil

        for putReply.Err != OK {
          
          ok := call(pb.backup, "PBServer.Put", putArgs, &putReply)
          if ok == false {
            // DPrintf("[Err] Fail to call %s PBServer.Put, Tag %s ...\n", pb.backup, tag)
          }

          time.Sleep(viewservice.PutInterval)
        }

      }

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
      // fmt.Printf("[INFO] Try Put(%s, %s) To %s ...\n", key, value, pb.me)
      pb.commit.Wait()

      if value != pb.commitValue {
        pb.commit.Add(1)
        // DPrintf("[INFO] %s, value %s != commit value %s ...\n", pb.me, value, pb.commitValue)
        // not yet commit
        var putReply PutReply
        putReply.Err = Nil

        if pb.backup != "" {
          for putReply.Err != OK {
            // DPrintf("[INFO] Try PutHash(%s, %s) To %s...\n", key, value, pb.backup)
            ok := call(pb.backup, "PBServer.Put", args, &putReply)
            if ok == false {
              // DPrintf("[Err] Fail to call %s PBServer.Put, Tag %s ...\n", pb.backup, tag)
            }
          }

          time.Sleep(viewservice.PutInterval)
        }
        
        previousValue, ok := pb.db[key]
        
        if ok == true {
          newValue := hash(previousValue + value)
          pb.db[key] = strconv.Itoa(int(newValue))
          // fmt.Printf("[INFO] %s Change1 commit value %s -> %s ...\n", pb.me, pb.commitValue, value)
          pb.commitValue = value
          pb.previousValue = previousValue

          reply.PreviousValue = previousValue
          reply.Err = OK

          DPrintf("[INFO] Update server %s (%s, %s) ,return value %s, commit value %s, new value %s ...\n", pb.me, key, value, reply.PreviousValue, pb.commitValue, pb.db[key])
        } else {

          newValue := hash("" + value)
          pb.db[key] = strconv.Itoa(int(newValue))
          // fmt.Printf("[INFO] %s Change2 commit value %s -> %s ...\n", pb.me, pb.commitValue, value)
          pb.commitValue = value
          pb.previousValue = ""

          reply.PreviousValue = ""
          reply.Err = OK
        
          DPrintf("[INFO] Update server %s (%s, %s) ,return value %s, commit value %s, new value %s ...\n", pb.me, key, value, reply.PreviousValue, pb.commitValue, pb.db[key])
        }

        pb.commit.Done()
      } else {
        // already commit
        // DPrintf("[INFO] (%s, %s) already commit, return %s ...\n", key, value, pb.previousValue)
        reply.PreviousValue = pb.previousValue
        reply.Err = OK
      }

    }
  } else if tag == Init {
    // Init
    if pb.init == false {
      pb.db = args.DB
      reply.Err = OK
    } else {
      reply.Err = OK
    }
    
    
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
      DPrintf("[INFO] Update server %s key %s , value %s ...\n", pb.me, key, pb.db[key])
      reply.Err = OK
      reply.PreviousValue = ""
  
      return nil

    } else{
      // Put Hash
      DPrintf("[INFO] Try Put(%s, %s) To %s ...\n", key, value, pb.me)
      pb.commit.Wait()
      if value != pb.commitValue {
        pb.commit.Add(1)
        // DPrintf("[INFO] %s value %s != commit value %s ...\n", pb.me, value, pb.commitValue)
        // not yet commit
        previousValue, ok := pb.db[key]
        
        if ok == true {
          newValue := hash(previousValue + value)
          pb.db[key] = strconv.Itoa(int(newValue))
          pb.commitValue = value
          pb.previousValue = previousValue
  
          reply.PreviousValue = previousValue
          reply.Err = OK
          DPrintf("[INFO] Update server %s key %s , value %d ...\n", pb.me, key, newValue)
        } else {
          newValue := hash("" + value)
          pb.db[key] = strconv.Itoa(int(newValue))
          pb.commitValue = value
          pb.previousValue = ""
  
          reply.PreviousValue = ""
          reply.Err = OK
          DPrintf("[INFO] Update server %s key %s , value %d ...\n", pb.me, key, newValue)
        }

        pb.commit.Done()
      } else {  
        // already commit
        // DPrintf("[INFO] (%s, %s) already commit, return %s ...\n", key, value, pb.previousValue)
        reply.PreviousValue = pb.previousValue
        reply.Err = OK
      }
    }
  }

  return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  key := args.Key

  reply.Viewnum = pb.viewnum

  if pb.dead == false && pb.role == Primary {
    var getReply GetReply
    DPrintf("[INFO] %s backup is %s ...\n", pb.me, pb.backup)
    if pb.backup != "" {
      DPrintf("[INFO] %s Send Request Get To Backup %s ...\n", pb.me, pb.backup)
      ok := call(pb.backup, "PBServer.Get", args, &getReply)
      if ok == false {
        DPrintf("[INFO] Call %s Get Fail ...\n", pb.backup)
      }

      DPrintf("[INFO] getReply.Viewnum %d, pb.viewnum %d ...\n", getReply.Viewnum, pb.viewnum)
      if getReply.Viewnum > pb.viewnum {
        reply.Err = ErrWrongServer
        return nil
      }
    }
    
    value, ok := pb.db[key]
    if ok {
      reply.Value = value
      reply.Viewnum = pb.viewnum
      reply.Err = OK
    } else {
      reply.Value = ""
      reply.Viewnum = pb.viewnum
      reply.Err = ErrNoKey
    }
   

  } else {
    reply.Err = ErrWrongServer

  }
  DPrintf("[INFO] %s Get Reply Value %s ...\n", pb.me, reply.Value)

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

      if pb.backup != "" {
         // Init Backup

         args := &PutArgs{}
         args.DB = pb.db
         args.Tag = Init
         var reply PutReply
         reply.Err = Nil
   
         for reply.Err != OK {
           ok := call(pb.backup, "PBServer.Put", args, &reply)
           if ok == false {
             DPrintf("[INFO] Fail to call %s Put ...\n", pb.backup)
           }
   
           time.Sleep(viewservice.PutInterval)
         }
         
      } else {
         //do nothing 
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
  pb.commitValue = ""
  pb.previousValue = ""
  pb.db = make(map[string]string)
  pb.init = false

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
          DPrintf("[Err] %s Discard the request ...\n", pb.me)
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          DPrintf("[Err] %s Process the request but force discard of reply ...\n", pb.me)
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
          // DPrintf("[Err] %s Normal ...\n", pb.me)
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
  DPrintf("[INFO] Start Server %s ...\n", pb.me)
  return pb
}
