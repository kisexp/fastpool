package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrPoolNotRunning = errors.New("the pool is not running")
	ErrJobNotFunc     = errors.New("generic worker not given a func()")
	ErrWorkerClosed   = errors.New("worker was closed")
	ErrJobTimedOut    = errors.New("job request timed out")
)

type Worker interface {
	Process(interface{}) interface{}
	BlockUntilReady()
	Interrupt()
	Terminate()
}

type closureWorker struct {
	processor func(interface{}) interface{}
}

type workerWrapper struct {
	worker        Worker
	interruptChan chan struct{}
	reqChan       chan<- workReq
	closeChan     chan struct{}
	closedChan    chan struct{}
}

func newWorkerWrapper(
	reqChan chan<- workReq,
	worker Worker,
) *workerWrapper {
	w := workerWrapper{
		worker:        worker,
		interruptChan: make(chan struct{}),
		reqChan:       reqChan,
		closeChan:     make(chan struct{}),
		closedChan:    make(chan struct{}),
	}

	go w.run()
	return &w
}

func (w *workerWrapper) interrupt() {
	close(w.interruptChan)
	w.worker.Interrupt()
}

func (w *workerWrapper) run() {
	jobChan, retChan := make(chan interface{}), make(chan interface{})
	defer func() {
		w.worker.Terminate()
		close(retChan)
		close(retChan)
		close(w.closedChan)
	}()
	for {
		w.worker.BlockUntilReady()
		select {
		case w.reqChan <- workReq{
			jobChan:       jobChan,
			retChan:       retChan,
			interruptFunc: w.interrupt,
		}:
			select {
			case payload := <-jobChan:
				result := w.worker.Process(payload)
				select {
				case retChan <- result:
				case <-w.interruptChan:
					w.interruptChan = make(chan struct{})
				}
			case _, _ = <-w.interruptChan:
				w.interruptChan = make(chan struct{})
			}
		case <-w.closedChan:
			return
		}
	}
}

func (w *workerWrapper) stop() {
	close(w.closedChan)
}

func (w *workerWrapper) join() {
	<-w.closedChan
}

func (w *closureWorker) Process(payload interface{}) interface{} {
	return w.processor(payload)
}

func (w *closureWorker) BlockUntilReady() {}
func (w *closureWorker) Interrupt()       {}
func (w *closureWorker) Terminate()       {}

type Pool struct {
	queuedJobs int64            // pool当前积压的job数量
	ctor       func() Worker    // worker具体的构造函数
	workers    []*workerWrapper // pool实际拥有的worker
	reqChan    chan workReq     // pool与所有worker进行通信的管道，所有worker与pool都使用相同的reqChan指针
	workerMut  sync.Mutex       // pool进行SetSize操作时使用的，防止不同协程同时对size进行操作
}

type workReq struct {
	jobChan       chan<- interface{}
	retChan       <-chan interface{}
	interruptFunc func()
}

type workWrapper struct {
}

func New(n int, ctor func() Worker) *Pool {
	p := &Pool{
		ctor:    ctor,
		reqChan: make(chan workReq),
	}
	p.SetSize(n)
	return p

}

func NewFunc(n int, f func(interface{}) interface{}) *Pool {
	return New(n, func() Worker {
		return &closureWorker{
			processor: f,
		}
	})
}

func NewCallback(n int) *Pool {
	return New(n, func() Worker {
		return &closureWorker{}
	})
}

func (p *Pool) Process(payload interface{}) interface{} {
	atomic.AddInt64(&p.queuedJobs, 1)
	req, open := <-p.reqChan
	if !open {
		panic(ErrPoolNotRunning)
	}
	req.jobChan <- payload
	payload, open = <-req.retChan
	if !open {
		panic(ErrWorkerClosed)
	}
	atomic.AddInt64(&p.queuedJobs, -1)
	return payload

}

func (p *Pool) ProcessTimed(
	payload interface{},
	timeout time.Duration,
) (interface{}, error) {
	atomic.AddInt64(&p.queuedJobs, 1)
	defer atomic.AddInt64(&p.queuedJobs, -1)
	tout := time.NewTimer(timeout)
	var req workReq
	var open bool

	select {
	case payload, open = <-req.retChan:
		if !open {
			return nil, ErrPoolNotRunning
		}
	case <-tout.C:
		return nil, ErrJobTimedOut
	}

	select {
	case req.jobChan <- payload:
	case <-tout.C:
		req.interruptFunc()
		return nil, ErrJobTimedOut
	}

	select {
	case payload, open = <-req.retChan:
		if !open {
			return nil, ErrWorkerClosed
		}
	case <-tout.C:
		req.interruptFunc()
		return nil, ErrJobTimedOut
	}
	tout.Stop()
	return payload, nil

}

func (p *Pool) ProcessCtx(ctx context.Context, payload interface{}) (interface{}, error) {
	atomic.AddInt64(&p.queuedJobs, 1)
	defer atomic.AddInt64(&p.queuedJobs, -1)

	var req workReq
	var open bool

	select {
	case req, open = <-p.reqChan:
		if !open {
			return nil, ErrPoolNotRunning
		}
	case <-ctx.Done():
		return nil, ctx.Err()

	}

	select {
	case req.jobChan <- payload:
	case <-ctx.Done():
		req.interruptFunc()
		return nil, ctx.Err()

	}
	return payload, nil
}

func (p *Pool) QueueLength() int64 {
	return atomic.LoadInt64(&p.queuedJobs)
}

func (p *Pool) SetSize(n int) {
	p.workerMut.Lock()
	defer p.workerMut.Unlock()
	lWorkers := len(p.workers)
	if lWorkers == n {
		return
	}

	for i := lWorkers; i < n; i++ {
		p.workers = append(p.workers, newWorkerWrapper(p.reqChan, p.ctor()))
	}

	for i := n; i < lWorkers; i++ {
		p.workers[i].stop()
	}

	for i := n; i < lWorkers; i++ {
		p.workers[i].join()
	}

	p.workers = p.workers[:n]
}

func (p *Pool) GetSize() int {
	p.workerMut.Lock()
	defer p.workerMut.Unlock()
	return len(p.workers)
}

func (p *Pool) Close() {
	p.SetSize(0)
	close(p.reqChan)
}

func main() {
}
