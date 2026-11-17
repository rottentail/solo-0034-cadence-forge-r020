package media

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func sh(args ...string) (string, []string) {
	return "/bin/sh", append([]string{"-c"}, args...)
}

func alive(pid int) bool {
	return syscall.Kill(pid, 0) == nil
}

// waitDead polls because a freshly exited PID may remain a zombie to kill -0
// until its parent reaps it; the lifecycle guarantees reaping via cmd.Wait, so
// the assertion only needs bounded patience rather than a single sample.
func waitDead(t *testing.T, pids ...int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		survivors := 0
		for _, pid := range pids {
			if alive(pid) {
				survivors++
			}
		}
		if survivors == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, pid := range pids {
		if alive(pid) {
			t.Errorf("pid %d survived termination", pid)
		}
	}
}

func childPids(t *testing.T, pgid int) []int {
	t.Helper()
	entries, err := os.ReadDir("/proc")
	if err != nil {
		t.Skipf("/proc unavailable: %v", err)
	}
	var out []int
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		stat, err := os.ReadFile(filepath.Join("/proc", e.Name(), "stat"))
		if err != nil {
			continue
		}
		// /proc/<pid>/stat: pid (comm) state ppid pgrp ...
		fields := strings.Fields(string(stat))
		if len(fields) < 5 {
			continue
		}
		pgrp, _ := strconv.Atoi(fields[4])
		if pgrp == pgid {
			out = append(out, pid)
		}
	}
	return out
}

// A context cancellation kills the whole process group and leaves no orphans.
func TestProcessCancellationReapsGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	// set -m forces job control so the backgrounded sleeper joins the shell's
	// process group; the lifecycle then reaps it via the negative group id.
	binary, args := sh("set -m; sleep 30 & child=$!; echo $child; kill -0 $child; wait")
	var errbuf LimitedBuffer
	errbuf.Limit = 1024
	p, err := Start(ctx, binary, args, nil, &errbuf)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	pgid := p.cmd.Process.Pid
	deadline := time.Now().Add(3 * time.Second)
	var members []int
	for time.Now().Before(deadline) {
		if pids := childPids(t, pgid); len(pids) >= 2 {
			members = pids
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(members) < 2 {
		t.Fatalf("group %d never contained shell and child: %v", pgid, members)
	}
	for _, pid := range members {
		if !alive(pid) {
			t.Fatalf("pid %d expected alive before cancellation", pid)
		}
	}
	cancel()
	if err = p.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait after cancel: got %v, want context.Canceled", err)
	}
	// Leader and every group member must be reaped, including the sleep child.
	waitDead(t, members...)
}

// Terminating one request's process must not signal a sibling process: each
// Process runs in its own group and kills by negative group id only.
func TestProcessIsolation(t *testing.T) {
	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	b1, a1 := sh("sleep 30")
	b2, a2 := sh("sleep 30")
	p1, err := Start(ctx1, b1, a1, nil, nil)
	if err != nil {
		t.Fatalf("start p1: %v", err)
	}
	p2, err := Start(ctx2, b2, a2, nil, nil)
	if err != nil {
		t.Fatalf("start p2: %v", err)
	}
	if p1.cmd.Process.Pid == p2.cmd.Process.Pid || p1.cmd.Process.Pid == -p2.cmd.Process.Pid {
		t.Fatal("process groups not isolated")
	}
	cancel1()
	if err = p1.Wait(ctx1); !errors.Is(err, context.Canceled) {
		t.Fatalf("p1 Wait: %v", err)
	}
	if !alive(p2.cmd.Process.Pid) {
		t.Fatal("sibling process was killed by another request's cancellation")
	}
	cancel2()
	if err = p2.Wait(ctx2); !errors.Is(err, context.Canceled) {
		t.Fatalf("p2 cancellation semantics wrong: got %v, want context.Canceled", err)
	}
}

// The deadline policy is the caller's. A short context surfaces
// DeadlineExceeded and reaps the process; Run remains a thin composition.
func TestRunTimeoutReapsProcess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	binary, args := sh("sleep 30")
	start := time.Now()
	_, err := Run(ctx, binary, args, 1024)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run: got %v, want DeadlineExceeded", err)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("timeout did not release the process promptly")
	}
}

// A process that exits 0 on its own reports success even if its context is
// still open; a non-zero exit reports the process error, not a context error.
func TestNaturalOutcomes(t *testing.T) {
	ctx := context.Background()
	p, err := Start(ctx, "/bin/sh", []string{"-c", "printf hello"}, nil, nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err = p.Wait(ctx); err != nil {
		t.Fatalf("clean exit: %v", err)
	}
	b, err := Run(ctx, "/bin/sh", []string{"-c", "echo boom 1>&2; exit 7"}, 64)
	if err == nil || b != nil {
		t.Fatalf("expected bounded error from failing process, got %v / %q", err, b)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("process failure misclassified as context error: %v", err)
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Fatalf("stderr not attached to process error: %v", err)
	}
}

// Concurrent lifecycles starting and being cancelled together must each reap
// only their own group — the media resource the instance shares.
func TestConcurrentLifecycles(t *testing.T) {
	const n = 12
	var wg sync.WaitGroup
	errs := make(chan error, n)
	procs := make([]*Process, n)
	cancels := make([]context.CancelFunc, n)
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		binary, args := sh(fmt.Sprintf("sleep %d", 30+i))
		p, err := Start(ctx, binary, args, nil, nil)
		if err != nil {
			t.Fatalf("start %d: %v", i, err)
		}
		procs[i], cancels[i] = p, cancel
	}
	for i := range procs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cancels[i]()
			if err := procs[i].Wait(context.Background()); !errors.Is(err, context.Canceled) {
				errs <- fmt.Errorf("proc %d: %w", i, err)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	var pending []int
	for _, p := range procs {
		if alive(p.cmd.Process.Pid) || len(childPids(t, p.cmd.Process.Pid)) != 0 {
			pending = append(pending, p.cmd.Process.Pid)
		}
	}
	waitDead(t, pending...)
}
