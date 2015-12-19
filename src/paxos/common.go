package paxos

type Instance struct {
	Seq int
	V interface{}
	tmpV interface{}

	Count int // proposer, count that agree to the instance
	OK bool // whether application agree to the instance
}

type PreArgs struct {
	Seq int
	V interface{}
}

type PreReply struct {
	OK bool
	Maxapt int 
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