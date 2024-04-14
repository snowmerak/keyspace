package keyspace

import (
	"errors"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

type Keyspace struct {
	clusterInfo        gocql.ClusterConfig
	sessions           []atomic.Pointer[gocql.Session]
	sessionFailed      []atomic.Int64
	maxRetriesPerQuery int64
	maxRetriesPerConn  int64
}

func New(clusterInfo gocql.ClusterConfig, maxRetriesPerQuery, maxRetriesPerConn int64) (*Keyspace, error) {
	numConns := clusterInfo.NumConns
	sessions := make([]atomic.Pointer[gocql.Session], numConns)

	for i := 0; i < numConns; i++ {
		cs, err := clusterInfo.CreateSession()
		if err != nil {
			return nil, fmt.Errorf("failed to create session: %v", err)
		}
		sessions[i].Store(cs)
	}

	return &Keyspace{
		clusterInfo:        clusterInfo,
		sessions:           sessions,
		sessionFailed:      make([]atomic.Int64, numConns),
		maxRetriesPerQuery: maxRetriesPerQuery,
		maxRetriesPerConn:  maxRetriesPerConn,
	}, nil
}

func (k *Keyspace) Query(action func(session *gocql.Session) error) error {
	for i := int64(0); i < k.maxRetriesPerQuery; i++ {
		sess, whenFailed, err := k.session()
		if err != nil {
			return fmt.Errorf("failed to get session: %v", err)
		}

		if err := action(sess); err != nil {
			switch {
			case errors.Is(err, gocql.ErrHostAlreadyExists):
				whenFailed(err)
				continue
			default:
				return fmt.Errorf("failed to execute query: %v", err)
			}
		}

		return nil
	}

	return fmt.Errorf("failed to execute query")
}

func (k *Keyspace) session() (sess *gocql.Session, whenFailed func(error), err error) {
	sl := int64(len(k.sessions))
	for i := int64(0); i < sl*2/3; i++ {
		index := rand.Int63n(sl)
		sess := k.sessions[index].Load()
		if sess != nil {
			return sess, k.closureWhenFailed(index), nil
		}
	}

	return nil, nil, fmt.Errorf("no sessions available")
}

const MaxDelay = 3 * time.Second

func (k *Keyspace) closureWhenFailed(i int64) func(error) {
	return func(err error) {
		if err == nil {
			return
		}

		if k.sessionFailed[i].Add(1) > k.maxRetriesPerConn {
			k.sessions[i].Store(nil)
		}

		if k.sessionFailed[i].Swap(0) < k.maxRetriesPerConn {
			return
		}

		cs, err := k.clusterInfo.CreateSession()
		delay := time.Millisecond * 10
		for err != nil {
			if delay < MaxDelay {
				delay *= 2
			}
			cs, err = k.clusterInfo.CreateSession()
		}

		k.sessions[i].Store(cs)
	}
}

func (k *Keyspace) Close() {
	for i := range k.sessions {
		s := k.sessions[i].Load()
		if s != nil {
			s.Close()
		}
	}
}
