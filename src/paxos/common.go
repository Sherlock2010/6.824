package paxos

type Instance struct {
	Seq int
	Count int
	OK bool
	V interface{}
}

type PreArgs struct {
	Seq int
	V interface{}
}

type PreReply struct {
	OK bool
	Seq int
	V interface{} // Value application can choose
}

type MaxArgs struct {

}

type MaxReply struct {
	Seq int
}

type AcceptArgs struct {
	Seq int
	V interface{}
}

type AcceptReply struct {
	OK bool
}