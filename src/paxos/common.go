package paxos

type Instance struct {
	Seq int
	Num string // unique number, ascending order with time
	V interface{}
	tmpV interface{}

	OK bool // whether application agree to the instance
}

type PreArgs struct {
	Seq int
	V interface{}
	Num string
}

type PreReply struct {
	OK bool
	Maxapt string 
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
	Num string
}

type AcceptReply struct {
	Num string
	OK bool
}

type DecisionArgs struct {
	Me int 
	Seq int
	Num string
	V interface{}
	Decided bool
	MaxDone int // max seq this peer holds
} 

type DecisionReply struct {
	MaxDone int // max seq other peer holds
} 