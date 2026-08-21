package raft

import (
	"math/rand"
	"time"
)

func RandomElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(201)+100) * time.Millisecond
}
