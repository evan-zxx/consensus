package paxos

// MsgType represents the type of a paxos phase.
type MsgType uint8

var TpStr = [4]string{"Prepare", "Promise", "Propose", "Accept"}

const (
	Prepare MsgType = iota
	Promise
	Propose
	Accept
)

type message struct {
	tp     MsgType
	from   int
	to     int
	number int    // proposal number
	value  string // proposal value
}
