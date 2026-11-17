package media

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

const Formats = "mov,matroska,webm,wav,mp3,flac,ogg,avi,mpegts,aac"

// terminationGrace is lifecycle plumbing, not a business policy: after a
// context ends the child gets one bounded chance to flush and leave its
// process group cleanly before the group is force-killed. Business deadlines
// stay owned by each caller (probe, visual, loudness, encode).
const terminationGrace = time.Second

type LimitedBuffer struct {
	bytes.Buffer
	Limit int
}

func (b *LimitedBuffer) Write(p []byte) (int, error) {
	n := len(p)
	remaining := b.Limit - b.Len()
	if remaining > 0 {
		if len(p) > remaining {
			p = p[:remaining]
		}
		b.Buffer.Write(p)
	}
	return n, nil
}

// Process owns one operating-system process lifecycle boundary: start, wait
// and termination. It deliberately knows nothing about ffprobe versus ffmpeg,
// timeouts or output contracts — those policies stay with the calling
// operation, which supplies an already-deadlined context and its own buffers.
type Process struct {
	cmd *exec.Cmd

	mu      sync.Mutex
	waitErr error
	cause   error
	done    chan struct{}
	stop    sync.Once
}

// Start launches binary with args in its own process group so terminating it
// can never signal a sibling request's process. stdout/stderr writers are
// optional and typically bounded by the caller.
func Start(ctx context.Context, binary string, args []string, stdout, stderr *LimitedBuffer) (*Process, error) {
	cmd := exec.Command(binary, args...)
	// A nil buffer means the operation does not capture that stream; do not
	// hand exec a typed-nil interface (its promoted ReadFrom would panic).
	if stdout != nil {
		cmd.Stdout = stdout
	} else {
		cmd.Stdout = io.Discard
	}
	if stderr != nil {
		cmd.Stderr = stderr
	} else {
		cmd.Stderr = io.Discard
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("media process: %w", err)
	}
	p := &Process{cmd: cmd, done: make(chan struct{})}
	go func() {
		err := cmd.Wait()
		p.mu.Lock()
		p.waitErr = err
		p.mu.Unlock()
		close(p.done)
	}()
	go func() {
		select {
		case <-ctx.Done():
			// Remember why termination was requested: Wait may be called with a
			// different (e.g. background) context and must still report the
			// original cancellation cause rather than a raw SIGTERM delivery.
			p.mu.Lock()
			p.cause = ctx.Err()
			p.mu.Unlock()
			p.terminate()
		case <-p.done:
		}
	}()
	return p, nil
}

// Wait blocks until the process exits. When the caller's context ended first,
// the context error is returned (matching exec.CommandContext semantics) so
// upload interruption and render cancellation remain distinguishable from a
// process that failed on its own; a clean exit reports nil.
func (p *Process) Wait(ctx context.Context) error {
	select {
	case <-p.done:
		return p.result()
	case <-ctx.Done():
		p.terminate()
		<-p.done
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.cause != nil {
			return p.cause
		}
		return ctx.Err()
	}
}

func (p *Process) result() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cause != nil {
		return p.cause
	}
	if p.waitErr == nil {
		return nil
	}
	return fmt.Errorf("media process: %w", p.waitErr)
}

// terminate is idempotent. The whole group is signalled, never an individual
// PID, so a child cannot be reaped twice nor another request's process be hit.
func (p *Process) terminate() {
	p.stop.Do(func() {
		pgid := p.cmd.Process.Pid
		_ = syscall.Kill(-pgid, syscall.SIGTERM)
		go func() {
			timer := time.NewTimer(terminationGrace)
			defer timer.Stop()
			select {
			case <-p.done:
			case <-timer.C:
				_ = syscall.Kill(-pgid, syscall.SIGKILL)
			}
		}()
	})
}

// Run is the bounded-output composition of the shared lifecycle: it starts a
// process, waits on the caller's context and enforces the stdout byte limit.
// Timeout and argument policy remain the caller's responsibility.
func Run(ctx context.Context, binary string, args []string, limit int) ([]byte, error) {
	var out, errout LimitedBuffer
	out.Limit = limit
	errout.Limit = 8192
	p, err := Start(ctx, binary, args, &out, &errout)
	if err != nil {
		return nil, err
	}
	if err = p.Wait(ctx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %s", err, errout.String())
	}
	if out.Len() >= limit {
		return nil, fmt.Errorf("media metadata exceeds limit")
	}
	return out.Bytes(), nil
}
