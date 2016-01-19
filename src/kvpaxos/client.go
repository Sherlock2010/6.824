package kvpaxos

import "net/rpc"
import "fmt"
import "math/rand"

type Clerk struct {
  servers []string
  // You will have to modify this struct.
}


func MakeClerk(servers []string) *Clerk {
  ck := new(Clerk)
  ck.servers = servers
  // You'll have to add code here.
  return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()
    
  err := c.Call(rpcname, args, reply)
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

    time.Sleep(RPCInterval)
    
    ok = call(srv, name, args, reply)
    count ++
  }

  DPrintf("[Succ] Call %s %s Succ ...\n", srv, name)
}

func (ck *Clerk) pick() int {
  ci := (rand.Int() % len(ck.servers)

  return ci
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
  // You will have to modify this function.
  args = &GetArgs{}
  var reply GetReply
  
  ci := ck.pick()
  server := ck.servers[ci]

  //at most once 
  sent = false
  for !sent {
   
    sent = true
    received := call(server, "server.Get", args, reply)
    // wait for acknowledgement with timeout
    if received && (reply.OK != OK) :
      sent = false
  }
      
  return reply.Value
}

//
// set the value for a key.
// keeps trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
  // You will have to modify this function.
  args = &PutArgs{}
  var reply PutReply
  
  ci := ck.pick()
  server := ck.servers[ci]

  //at most once 
  sent = false
  for !sent {
   
    sent = true
    received := call(server, "server.Put", args, reply)
    // wait for acknowledgement with timeout
    if received && (reply.OK != OK) :
      sent = false
  }

  return reply.PreviousValue
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true)
  return v
}
