package paxos

import (
	"log"
	"time"
)

type Entry struct {
	ack    bool
	number int
	value  string
}

type proposer struct {
	// server id
	id int
	// the largest round number the server has seen
	round int
	// proposal number = (round number, serverID)
	number int
	// proposal value
	value     string
	acceptors map[int]*Entry
	net       network
}

func newProposer(id int, value string, net network, acceptors ...int) *proposer {
	// 这里直接将value赋值给了proposer的value
	p := &proposer{id: id, round: 0, value: value, net: net, acceptors: make(map[int]*Entry, len(acceptors))}
	for _, acceptor := range acceptors {
		p.acceptors[acceptor] = new(Entry)
	}
	return p
}

func (p *proposer) run() {
	var ok bool
	var msg message
	// 直到过半的acceptor响应成功
	for !p.majorityReached() {
		if !ok {
			// send prepare
			prepareMsg := p.prepare()
			for i := range prepareMsg {
				p.net.send(prepareMsg[i])
			}
		}

		msg, ok = p.net.recv(time.Second)
		if !ok {
			// 1s超时, 则继续发送prepare
			continue
		}

		switch msg.tp {
		case Promise:
			p.handlePromise(msg)
		default:
			panic("UnSupport message.")
		}
	}

	// 如果第一阶段prepare()收到了过半的promise回复
	// 则进入第二阶段 send accept
	proposeMsg := p.accept()
	for _, msg := range proposeMsg {
		p.net.send(msg)
	}
}

// Phase 1. (a) A proposer selects a proposal number n
// and sends a prepare request with number n to a majority of acceptors.
func (p *proposer) prepare() []message {
	p.round++
	p.number = p.proposalNumber()
	//msg := make([]message, p.majority())
	msg := make([]message, p.acceptorCnt())
	i := 0

	for to := range p.acceptors {
		msg[i] = message{
			tp:     Prepare,
			from:   p.id,
			to:     to,
			number: p.number,
			value:  "", // prepare中不关心value
		}
		i++
		// TODO: 第一阶段prepare 为什么只发给majority?  而不是所有的acceptor
		//if i == p.majority() {
		//	break
		//}
	}
	return msg
}

func (p *proposer) handlePromise(reply message) {
	//log.Printf("proposer: %d received a promise %+v", p.id, reply)
	acceptor, ok := p.acceptors[reply.from]
	if !ok {
		log.Print("can't find acceptor in proposer.")
	}
	// 如果acceptor回复的acceptorNumber有值(曾经已经accept过值)
	// TODO: 这里应该是判断reply.number>p.number吧?? 如果每个accept分开存就可以直接判断>0
	//if reply.number > 0 {
	//	acceptor.number = reply.number
	//	acceptor.value = reply.value
	//	//p.number = reply.number
	//	//p.value = reply.value
	//}
	acceptor.ack = true
	acceptor.number = reply.number
	acceptor.value = reply.value
}

// 实际上是发送accept包, 且发给acceptors的accept包不会再收到回包
// Phase 2. (a) If the proposer receives a response to its prepare requests (numbered n)
// from a majority of acceptors, then it sends an accept request to each of those acceptors
// for a proposal numbered n with a value v, where v is the value of the highest-numbered proposal
// among the responses, or is any value if the responses reported no proposals.
func (p *proposer) accept() []message {
	//msg := make([]message, p.majority())
	msg := make([]message, p.acceptorCnt())
	i := 0
	number, value := p.getPreAcceptEntry()
	for to, acceptor := range p.acceptors {
		if acceptor.ack {
			msg[i] = message{
				tp:     Propose, //accept
				from:   p.id,
				to:     to,
				number: number,
				value:  value,
			}
			i++
		}

		//if i == p.majority() {
		//	break
		//}
	}
	return msg
}

func (p *proposer) majority() int {

	// 超过半数即可进入下个流程.
	//return len(p.acceptors)/2 + 1

	// 需要所有实例都success.
	return len(p.acceptors)
}

func (p *proposer) majorityReached() bool {
	count := 0
	for _, acceptor := range p.acceptors {
		if acceptor.ack {
			count++
		}
	}
	if count >= p.majority() {
		return true
	}
	return false
}

// proposal number = (round number, serverID)
func (p *proposer) proposalNumber() int {
	return p.round<<16 | p.id
}

func (p *proposer) acceptorCnt() int {
	return len(p.acceptors)
}

func (p *proposer) getPreAcceptEntry() (int, string) {
	number := p.number
	value := p.value
	for _, v := range p.acceptors {
		if v.number > number {
			number = v.number
			value = v.value
		}
	}
	return number, value
}
