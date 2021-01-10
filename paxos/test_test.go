package paxos

import (
	"testing"
	"time"
)

func TestSingleProposer(t *testing.T) {
	// 1, 2, 3 are acceptors
	// 1001 is a proposer
	// 2001 is leaner
	nt := newNetwork(1, 2, 3, 1001, 2001)

	acceptors := make([]*acceptor, 0)
	for i := 1; i <= 3; i++ {
		acceptors = append(acceptors, newAcceptor(i, nt.nodeNetwork(i), 2001))
	}

	for _, a := range acceptors {
		go a.run()
	}

	time.Sleep(time.Millisecond)

	p := newProposer(1001, "hello world", nt.nodeNetwork(1001), 1, 2, 3)
	go p.run()

	l := newLearner(2001, nt.nodeNetwork(2001), 1, 2, 3)
	value := l.learn()
	if value != "hello world" {
		t.Errorf("value = %s, excepted %s", value, "hello world")
	}

	time.Sleep(time.Millisecond)
	t.Logf("---------------------------------------------\n")

	t.Logf("proposer:%d stat:%+v", p.id, p)
	for k, v := range p.acceptors {
		t.Logf("proposer's acceptors:%d stat:%+v", k, v)
	}
	t.Logf("---------------------------------------------\n")

	// print acceptor's status
	for k, v := range acceptors {
		t.Logf("acceptors:%d stat:%+v", k, v)
	}
}

func TestTwoProposers(t *testing.T) {
	// 1, 2, 3 are acceptors
	// 1001,1002 is a proposer
	nt := newNetwork(1, 2, 3, 1001, 1002, 1003, 2001)

	as := make([]*acceptor, 0)
	for i := 1; i <= 3; i++ {
		as = append(as, newAcceptor(i, nt.nodeNetwork(i), 2001))
	}

	for _, a := range as {
		go a.run()
	}

	p1 := newProposer(1001, "111", nt.nodeNetwork(1001), 1, 2, 3)
	//go p1.run()

	//time.Sleep(time.Millisecond)

	p2 := newProposer(1002, "222", nt.nodeNetwork(1002), 1, 2, 3)
	//go p2.run()

	p3 := newProposer(1003, "333", nt.nodeNetwork(1003), 1, 2, 3)

	go p1.run()

	go p3.run()
	go p2.run()

	l := newLearner(2001, nt.nodeNetwork(2001), 1, 2, 3)
	value := l.learn()
	if value != "111" {
		//t.Errorf("value = %s, want %s", value, "111")
	}
}
