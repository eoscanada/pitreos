package llerrgroup

import (
	"sync"

	"golang.org/x/sync/errgroup"
)

type Group struct {
	errgroup.Group
	queue     chan bool
	quit      chan struct{}
	closeLock sync.Mutex

	callsCount int
}

func New(parallelOperations int) *Group {
	return &Group{
		queue: make(chan bool, parallelOperations),
		quit:  make(chan struct{}, 0),
	}
}

// Stop blocks for a free queue position, and returns whether you should stop processing requests.  In a for loop
func (g *Group) Stop() bool {
	select {
	case g.queue <- true:
		return false
	case <-g.quit:
		return true
	}
}
func (g *Group) CallsCount() int {
	return g.callsCount
}

func (g *Group) Go(f func() error) {
	g.Group.Go(func() error {
		err := f()

		g.closeLock.Lock()

		g.callsCount++
		select {
		case <-g.quit:
		default:
			if err != nil {
				close(g.quit)
			}
		}
		g.closeLock.Unlock()

		<-g.queue
		return err
	})
}
