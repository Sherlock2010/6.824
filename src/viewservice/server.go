
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
  currentView *View //already acked
  nextView *View //prepared acked
  
  currentAck bool
  nextAck bool // currentView is acked
  ack bool
  isNew bool
  servers map[string]time.Time
}

func MakeView(Viewnum uint, Primary string, Backup string) *View {
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

  // Your code here.
  server := args.Me
  num := args.Viewnum
  
  if vs.currentView == nil {
    //first server in, init currentView and nextView
    vs.currentView = MakeView(1, server, "")
    vs.nextView = MakeView(1, server, "")

    vs.servers = make(map[string]time.Time)
    vs.servers[server] = time.Now()
    vs.ack = false
    vs.isNew = false
    reply.View = *(vs.currentView)
    // fmt.Printf("[Info] First primary %s in , view number %d ...\n", reply.View.Primary, reply.View.Viewnum)
    
  } else {
    //primary server already elected
    
    if num == 0 {
      //new server in or old server crashed, do the same thing
      reply.View = *(vs.currentView)

      // _, ok := vs.servers[server]

      if vs.nextView.Backup == "" {
        // vs.nextView.Backup += server + ";"
        vs.nextView.Backup += server
        vs.servers[server] = time.Now()

        vs.ack = false
        vs.isNew = true

        vs.nextView.Viewnum ++
        // fmt.Printf("[Info] %s become backup, primary is %s ...\n", server, vs.currentView.Primary)
      }
     
      
    } else {
      //exist server Ping
      vs.servers[server] = time.Now()

      if (server == vs.currentView.Primary) {
        // fmt.Printf("[Info] primary %s Ping , current view num %d ...\n", server, vs.currentView.Viewnum)
        
        if num == vs.currentView.Viewnum {
          vs.ack = true
          if vs.isNew {
            *(vs.currentView) = *(vs.nextView)
            reply.View = *(vs.currentView)

            vs.ack = false
            vs.isNew = false

            // fmt.Printf("[Info] change view, currentView num %d, ack %t ...\n", vs.currentView.Viewnum, vs.ack)
            // fmt.Printf("[Info] ack view number %d, primary is %s, backup is %s ...\n", vs.currentView.Viewnum, vs.currentView.Primary, vs.currentView.Backup)
          } else {
            // vs.ack = false
          }
          
        } else {
          vs.ack = false
        }
        
      } else {
        //backup Ping, do nothing
        reply.View = *(vs.currentView)
        
        // fmt.Printf("[Info] backup %s Ping, view num %d ...\n", server, num)
      }
         
    }  
  }

  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  // Your code here.
  if vs.currentView != nil {
    reply.View = *(vs.currentView)
  } else {
    // fmt.Printf("[Error] Get view error\n")
    view := MakeView(0, "", "")
    reply.View = *view
  }

  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

  // Your code here.
  for server, last := range vs.servers {      
      if (time.Now().Sub(last)) > DeadPings * PingInterval && vs.ack{
        vs.ack = false
        if server == vs.currentView.Primary {
          //server dead

          // fmt.Printf("[Info] primary %s dead , out of touch %d, dead interval is %d ...\n", server, time.Now().Sub(last), DeadPings * PingInterval)
          delete(vs.servers, server)
          vs.currentView.Primary = vs.currentView.Backup
          vs.currentView.Backup = ""
          vs.currentView.Viewnum ++

          *(vs.nextView) = *(vs.currentView)
        } else {
          //backup dead

          // fmt.Printf("[Info] backup %s dead , out of touch %d, dead interval is %d ...\n", server, time.Now().Sub(last), DeadPings * PingInterval)
          delete(vs.servers, server)
          vs.currentView.Backup = ""
          vs.currentView.Viewnum ++

          *(vs.nextView) = *(vs.currentView)
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
