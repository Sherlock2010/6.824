package pbservice

import "viewservice"
import "net/rpc"
import "fmt"

// You'll probably need to uncomment these:
import "time"
// import "crypto/rand"
// import "math/big"



type Clerk struct {
  vs *viewservice.Clerk
  // Your declarations here
}


func MakeClerk(vshost string, me string) *Clerk {
  ck := new(Clerk)
  ck.vs = viewservice.MakeClerk(me, vshost)
  // Your ck.* initializations here

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

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

  primary := ck.vs.Primary()
  // fmt.Printf("[INFO] Get(%s) ...\n", key)
  args := &GetArgs{}

  args.Key = key

  var reply GetReply
  reply.Err = Nil

  for ! (reply.Err == OK || reply.Err == ErrNoKey) {
    // send an RPC request, wait for the reply.
    // DPrintf("[INFO] Get(%s) ...\n", key)
    ok := call(primary, "PBServer.Get", args, &reply)
    if ok == false {
      primary = ck.vs.Primary()
      // DPrintf("[INFO] Get(%s) failed ...\n", key)
    }
    
    time.Sleep(viewservice.GetInterval)
    // DPrintf("[INFO] Try Get(%s) again ...\n", key)
  }
  
  value := reply.Value
  DPrintf("[INFO] Get(%s) value(%s) ...\n", key, value)
  return value

}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {

  primary := ck.vs.Primary()

  args := &PutArgs{}

  args.Key = key
  args.Value = value
  args.DoHash = dohash
  args.Tag = Update

  var reply PutReply
  reply.Err = Nil

  for reply.Err != OK {

    // send an RPC request, wait for the reply.
    ok := call(primary, "PBServer.Put", args, &reply)
    if ok == false {
      primary = ck.vs.Primary()
      DPrintf("[INFO] Put(%s, %s) to Primary %s failed ...\n", key, value, primary)
    }
    
    time.Sleep(viewservice.PutInterval)
  }
  DPrintf("[INFO] Put(%s, %s) to Primary %s succeed, reply %s ...\n", key, value, primary, reply.PreviousValue)
  previousValue := reply.PreviousValue

  return previousValue  
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true)
  return v
}
