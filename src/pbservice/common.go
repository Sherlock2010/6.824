package pbservice

import "hash/fnv"

const (
  // For Put/Get RPC
  Nil = "Nil"
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"

  Init = "Init"
  Update = "Update"
  Over = "Over"

  // For PBServer Role
  Primary = "Primary"
  Backup = "Backup"

)
type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool // For PutHash
  // You'll have to add definitions here.
  Tag string
  DB map[string]string
  // Field names must start with capital letters,
  // otherwise RPC will break.
}

type PutReply struct {
  Err Err
  PreviousValue string // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.

}

type GetReply struct {
  Err Err
  Value string
  Viewnum uint
}


// Your RPC definitions here.

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

