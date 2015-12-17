package paxos

type StartArgs struct {
	Seq int
	V interface{}
}

type StartReply struct {
	OK bool
	Seq int
	V interface{} // Value application can choose
}