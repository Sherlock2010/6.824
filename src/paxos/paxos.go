package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "time"
import "math/rand"
import "strconv"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string // paxos cluster
  me int // index into peers[]


  // Your data here.
  prepareDone sync.WaitGroup
  acceptDone sync.WaitGroup
  decisionDone sync.WaitGroup

  insMap map[int]*Instance
  done []int
}

func (px *Paxos) MakeInstance(seq int) {
  // init instance
  ins := &Instance{}
  
  ins.Seq = seq
  ins.V = nil

  ins.Maxpre = ""
  ins.Maxapt = ""
  ins.Maxaptv = nil

  ins.OK = false
  
  px.insMap[seq] = ins
  

}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()
  
  err = c.Call(name, args, reply)
  
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

func atMostOnce(srv string, name string, args interface{}, reply interface{}) {
  ok := call(srv, name, args, reply)
  count := 0

  for ok == false && count < 5 {
    DPrintf("[Err] Call %s %s Fail ...\n", srv, name)

    time.Sleep(Interval)
    
    ok = call(srv, name, args, reply)
    count ++
  }

  DPrintf("[Succ] Call %s %s Succ ...\n", srv, name)
}


//
// if n is majority of the peers
// 
func (px *Paxos) isMajority(n int) bool {
  return n >= ( len(px.peers) / 2 + 1 )
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  // Act as proposer to a instance

  _, ok := px.insMap[seq]    

  if ok == false {
    // init instance
    px.MakeInstance(seq)
    DPrintf("[INFO] %d start create ins %d ...\n", px.me, seq)
    // prepare thread
    go px.doProposer(seq, v)

  }

}

func (px *Paxos) doProposer(seq int, v interface{}) {
  for {
    DPrintf("[INFO] %d Start Proposer %d ...\n", px.me, seq)

    ok, acceptors, V := px.Prepare(seq, v)

    if ok == true {
      ok = px.Accept(seq, acceptors, V)
    }
    if ok == true {
      px.Decision(seq, V)

      break
    }
    
  }
  DPrintf("[INFO] %d End Proposer ...\n", px.me)
}

func (px *Paxos) Prepare(seq int, v interface{}) (bool, [] string, interface{}){
  ins := px.insMap[seq]
  ins.Num = px.Generate() 
  DPrintf("[INFO] %d Init %d Num %s Value %v ...\n", px.me, ins.Seq, ins.Num, v)

  args := &PreArgs{}
  args.Seq = ins.Seq
  // modify , not pass 5
  // args.V = v
  args.Num = ins.Num

  acceptors := make([]string, 0)
  count := 0
  maxN := ""
  maxV := v

  for i, peer := range px.peers {
    px.prepareDone.Add(1)

    if i == px.me {
      var reply PreReply
      go func(args *PreArgs, reply *PreReply) {
        // call local prepare
        DPrintf("[INFO] %d sent prepare to %s ...\n", px.me, peer)

        px.PrepareHandler(args, reply)

        px.prepareDone.Done()
        if reply.OK == true {
          acceptors = append(acceptors, peer)
          count ++
          px.mu.Lock()
          
          if reply.Maxapt > maxN && reply.V != nil{
            maxN = reply.Maxapt
            maxV = reply.V
          } 
          
          px.mu.Unlock()
        }
      }(args, &reply)
      
    } else {
      var reply PreReply
      go func(peer string, args *PreArgs, reply *PreReply) {
        // call remote prepare
        DPrintf("[INFO] %d sent Num %s prepare to %s ...\n", px.me, ins.Num, peer)
        
        atMostOnce(peer, "Paxos.PrepareHandler", args, &reply)

        px.prepareDone.Done()
        if reply.OK == true {
          acceptors = append(acceptors, peer)
          count ++
  
          px.mu.Lock()
          
          if reply.Maxapt > maxN {
            maxN = reply.Maxapt
            maxV = reply.V
          } 
          
          px.mu.Unlock()
    
        }

      }(peer, args, &reply)
    }
    
  }

  // wait until add rpc finish
  px.prepareDone.Wait()
  DPrintf("[INFO] %d receive prepare count %d ...\n", px.me, count)
  return px.isMajority(count), acceptors, maxV
}

func (px *Paxos) Accept(seq int, acceptors [] string, V interface{}) bool{

  ins := px.insMap[seq]

  args := &AcceptArgs{}
  args.Seq = ins.Seq
  args.V = V
  args.Num = ins.Num

  count := 0

  for i, peer := range acceptors {
    px.acceptDone.Add(1)

    if i == px.me {
      var reply AcceptReply
      go func (args *AcceptArgs, reply *AcceptReply) {
        DPrintf("[INFO] %d sent Num %s accept to %s ...\n", px.me, ins.Num, peer)
        
        px.AcceptHandler(args, reply)

        px.acceptDone.Done()
        if reply.OK == true {
          count ++
        }

      }(args, &reply)

    } else {
      var reply AcceptReply
      go func(peer string, args *AcceptArgs, reply *AcceptReply) {
        // accept   
        DPrintf("[INFO] %d sent accept to %s ...\n", px.me, peer)
    
        atMostOnce(peer, "Paxos.AcceptHandler", args, &reply)

        DPrintf("[Succ] Call AcceptHandler to %s Succ ...\n", peer)
        
        px.acceptDone.Done()
        if reply.OK == true {
          count ++
        }
        // px.insMap[seq].OK = reply.OK
      
      }(peer, args, &reply)
    }

  }
  px.acceptDone.Wait()
  DPrintf("[INFO] %d receive accept count %d ...\n", px.me, count)
  return px.isMajority(count)
}

func (px *Paxos) Decision(seq int, V interface{}) {
  DPrintf("[INFO] %d try get seq %d ...\n", px.me, seq)
  ins, ok := px.insMap[seq]
  if ok == false {
    DPrintf("[INFO] %d ins seq %d not exist ! ...\n", px.me, seq)
  }
  
  // px.done[px.me] = seq
  ins.V = V

  args := &DecisionArgs{}
  args.Seq = ins.Seq
  args.Num = ins.Num
  args.V = ins.V
  args.Decided = true
  args.Me = px.me
  args.MaxDone = px.done[px.me]

  var reply DecisionReply
  for i, peer := range (px.peers) {
    px.decisionDone.Add(1)
    if i == px.me {
      DPrintf("[INFO] %d sent Num %s decision %s to %s ...\n", px.me, ins.Num, args.Num, peer)

      go func(args *DecisionArgs, reply *DecisionReply) {
        px.DecisionHandler(args, reply)
        px.decisionDone.Done()
      }(args, &reply)
      
    } else {
      DPrintf("[INFO] %d sent decision %s to %s ...\n", px.me, args.Num, peer)

      go func(peer string, args *DecisionArgs, reply *DecisionReply) {

        atMostOnce(peer, "Paxos.DecisionHandler", args, &reply)
        
        px.done[i] = reply.MaxDone
        
        px.decisionDone.Done()
      }(peer, args, &reply) 
    } 
  }

  px.decisionDone.Wait()
  
}

// 
// Prepare handler
// 
func (px *Paxos) PrepareHandler(args *PreArgs, reply *PreReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  seq := args.Seq
  num := args.Num
  // v := args.V

  _, ok := px.insMap[seq] 

  if ok == false {
    px.MakeInstance(seq)
    DPrintf("[INFO] %d prepare handler create ins %d ...\n", px.me, seq)
  }

  ins := px.insMap[seq]
  reply.OK = false

  if num > ins.Maxpre {
    
    ins.Maxpre = num  
    // accept
    reply.OK = true
     
    if ins.Maxapt == "" {
      //init peer
      reply.V = nil
    } else {
      reply.Maxapt = ins.Maxapt 
      reply.V = ins.Maxaptv
    }      
    DPrintf("[INFO] %d prepare handle %s ...\n", px.me, num)
  } else {
    // reject  
    DPrintf("[INFO] %d prepare reject %s ...\n", px.me, num)
  }
  
  return nil
}

// 
// Accept handler
// 
func (px *Paxos) AcceptHandler(args *AcceptArgs, reply *AcceptReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  seq := args.Seq
  num := args.Num
  v := args.V

  _, ok := px.insMap[seq]
  if ok == false {
    px.MakeInstance(seq)
    DPrintf("[INFO] %d accepthandler create ins %d ...\n", px.me, seq)
  }

  ins := px.insMap[seq]

  reply.Num = args.Num
  reply.OK = false
 
  if num >= ins.Maxpre {
    
    ins.Maxapt = num
    ins.Maxaptv = v
  
    reply.OK = true

    px.insMap[seq].V = v
    // px.insMap[seq].OK = true
    DPrintf("[INFO] %d accept handle %s ...\n", px.me, num)
  } else {
    
    DPrintf("[INFO] %d accept reject %s ...\n", px.me, num)
  }
  
  return nil
}

func (px *Paxos) DecisionHandler(args *DecisionArgs, reply *DecisionReply) error {
    px.mu.Lock()
    defer px.mu.Unlock()
    seq := args.Seq
    decided := args.Decided
    num := args.Num
    peer := args.Me
    v := args.V
    _, ok := px.insMap[seq]
    if ok == false {
      px.MakeInstance(seq)
      DPrintf("[INFO] %d decisionhandler create ins %d ...\n", px.me, seq)
    }

    ins := px.insMap[seq]
    DPrintf("[INFO] %d Change %d V from %v to %v ...\n",px.me, seq, ins.V, args.V)
    ins.V = v // ?

    ins.OK = decided

    px.done[peer] = args.MaxDone
    reply.MaxDone = px.done[px.me]

    DPrintf("[INFO] %d decision handle %s ...\n", px.me, num)
    return nil
}


// generate unique number ascending order with time
func (px *Paxos) Generate() string {
  begin := time.Date(1976, time.January, 1, 0, 0, 0, 0, time.UTC)
  duration := time.Now().Sub(begin)
  return strconv.FormatInt(duration.Nanoseconds(), 10) + "-" + strconv.Itoa(px.me)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {

  if px.done[px.me] < seq {
    px.done[px.me] = seq
  }
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
  max := 0
  for _, ins := range px.insMap {
    if ins.Seq > max{
      max = ins.Seq
    }
  }
  return max

}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
  min := px.done[px.me]

  for _, seq := range px.done {
    if min > seq {
      min = seq
    }
  }
  // fmt.Printf("[INFO] %d Min %d ...\n", px.me, min)
  for seq, ins := range px.insMap {
    
    if ins.Seq < min && ins.OK {
      
      delete(px.insMap, seq)
      DPrintf("[INFO] %d delete Seq %d ...\n", px.me, seq)
    }
  }
  
  return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  
  ins, ok := px.insMap[seq]

  if ok == true {
    
    return ins.OK, ins.V
  } else {
   
    return false, nil
  }

}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me


  // Your initialization code here.

  px.insMap = make(map[int]*Instance)

  px.done = make([]int, len(peers))
  for i := range peers{
    px.done[i] = -1
  }

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l
    
    // please do not change any of the following code,
    // or do anything to subvert it.
    
    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
            
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}

