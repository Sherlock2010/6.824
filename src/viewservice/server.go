
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string

  // Your declarations here.
  curView *View
  idle string // idle server
  first bool
  ack bool
  servers map[string]time.Time
}

func MakeView(Viewnum uint, Primary string ,Backup string) *View {
  view := new(View)

  view.Viewnum = Viewnum
  view.Primary = Primary
  view.Backup = Backup

  return view
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

  server := args.Me
  num := args.Viewnum
  
  if vs.first {
    // viewservice first starts, accept any server as the first primary
    vs.curView.Primary = server
    vs.curView.Viewnum = 1

    
  } else {
    vs.servers[server] = time.Now()

    if num == 0 {
      // new server or old server re-start  
      if vs.curView.Backup == "" {
        if vs.ack {
          vs.curView.Backup = server
          vs.curView.Viewnum ++
          vs.ack = false
        } else {
          vs.idle = server
        }
      } else {
        // Backup not empty and receive idle server is impossible

      }

    } else {
      if num == vs.curView.Viewnum {
        vs.ack = true

        if vs.idle != "" {
          vs.curView.Backup = vs.idle
          vs.curView.Viewnum ++
          vs.ack = false
        }
      }
    }

  }
  reply.View = *(vs.curView)

  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  // Your code here.
  reply.View = *(vs.curView)
  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

  // Your code here.
  for server, t := range vs.servers {
    if time.Now().Sub(t) > DeadPings *PingInterval {
      // server dead
      if vs.ack {
        // if curView acked
        delete(vs.servers, server)

        if server == vs.curView.Primary {
          vs.curView.Primary = vs.curView.Backup
          vs.curView.Backup = ""
          vs.curView.Viewnum ++
          vs.ack = false
        } else {
          vs.curView.Backup = ""
          vs.curView.Viewnum ++
          vs.ack = false
        }
      } else {
        // if curView not acked, do nothing
      }
    }
  }
  
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  // Your vs.* initializations here.
  vs.curView = MakeView(0, "", "")
  vs.first = true
  vs.ack = false
  vs.servers = make(map[string]time.Time)
  vs.idle = ""

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
