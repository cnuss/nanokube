package storage

import (
	"context"
	"io"
	"sync"

	"google.golang.org/grpc/metadata"
)

// bidiPipe is an in-memory bidirectional stream shared by a client view and a
// server view. It splices clientv3's grpc streaming client (Watch,
// LeaseKeepAlive) onto the embedded etcd server's blocking stream handler with
// no grpc transport. ctx is the one clientv3 passed; cancelling it tears down
// both ends, mirroring grpc stream-context semantics.
type bidiPipe[Req, Resp any] struct {
	ctx   context.Context
	reqs  chan *Req  // client Send -> server Recv
	resps chan *Resp // server Send -> client Recv

	closeSend sync.Once
	done      chan struct{} // closed when the server handler returns

	mu  sync.Mutex
	err error // terminal error from the server handler
}

// spawnStream runs the server's blocking stream handler against a fresh pipe in
// a goroutine and returns the client end. The handler returns only once its recv
// loop sees an error (ctx cancel or CloseSend) and its send loop has drained;
// only then is it safe to close resps and signal client-side EOF.
func spawnStream[Req, Resp any](ctx context.Context, run func(bidiServer[Req, Resp]) error) bidiClient[Req, Resp] {
	p := &bidiPipe[Req, Resp]{
		ctx:   ctx,
		reqs:  make(chan *Req, 16),
		resps: make(chan *Resp, 16),
		done:  make(chan struct{}),
	}
	go func() {
		err := run(bidiServer[Req, Resp]{p})
		p.mu.Lock()
		p.err = err
		p.mu.Unlock()
		close(p.resps)
		close(p.done)
	}()
	return bidiClient[Req, Resp]{p}
}

// --- client view (satisfies pb.*_*Client: Send/Recv/CloseSend + grpc.ClientStream) ---

type bidiClient[Req, Resp any] struct{ *bidiPipe[Req, Resp] }

func (c bidiClient[Req, Resp]) Send(req *Req) error {
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	case c.reqs <- req:
		return nil
	}
}

func (c bidiClient[Req, Resp]) Recv() (*Resp, error) {
	select {
	case <-c.ctx.Done():
		return nil, c.ctx.Err()
	case resp, ok := <-c.resps:
		if !ok {
			c.mu.Lock()
			err := c.err
			c.mu.Unlock()
			if err == nil {
				return nil, io.EOF
			}
			return nil, err
		}
		return resp, nil
	}
}

func (c bidiClient[Req, Resp]) CloseSend() error {
	c.closeSend.Do(func() { close(c.reqs) })
	return nil
}

func (c bidiClient[Req, Resp]) Context() context.Context     { return c.ctx }
func (c bidiClient[Req, Resp]) Header() (metadata.MD, error) { return nil, nil }
func (c bidiClient[Req, Resp]) Trailer() metadata.MD         { return nil }
func (c bidiClient[Req, Resp]) SendMsg(m any) error          { return c.Send(m.(*Req)) }
func (c bidiClient[Req, Resp]) RecvMsg(m any) error {
	resp, err := c.Recv()
	if err != nil {
		return err
	}
	*m.(*Resp) = *resp
	return nil
}

// --- server view (satisfies pb.*_*Server: Send/Recv + grpc.ServerStream) ---

type bidiServer[Req, Resp any] struct{ *bidiPipe[Req, Resp] }

func (s bidiServer[Req, Resp]) Send(resp *Resp) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case s.resps <- resp:
		return nil
	}
}

func (s bidiServer[Req, Resp]) Recv() (*Req, error) {
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case req, ok := <-s.reqs:
		if !ok {
			return nil, io.EOF // client called CloseSend
		}
		return req, nil
	}
}

func (s bidiServer[Req, Resp]) Context() context.Context     { return s.ctx }
func (s bidiServer[Req, Resp]) SetHeader(metadata.MD) error  { return nil }
func (s bidiServer[Req, Resp]) SendHeader(metadata.MD) error { return nil }
func (s bidiServer[Req, Resp]) SetTrailer(metadata.MD)       {}
func (s bidiServer[Req, Resp]) SendMsg(m any) error          { return s.Send(m.(*Resp)) }
func (s bidiServer[Req, Resp]) RecvMsg(m any) error {
	req, err := s.Recv()
	if err != nil {
		return err
	}
	*m.(*Req) = *req
	return nil
}
