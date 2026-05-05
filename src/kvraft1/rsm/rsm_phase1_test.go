package rsm

import (
	"testing"
	"time"

	tester "6.5840/tester1"
)

// TestPhase1_OpStructHasUniqueID: compile-time check that Op struct có field UniqueID và Req.
// Hai Op với ID khác nhau phải có UniqueID khác nhau.
func TestPhase1_OpStructHasUniqueID(t *testing.T) {
	op1 := Op{UniqueID: 1, Req: Inc{}}
	op2 := Op{UniqueID: 2, Req: Inc{}}
	if op1.UniqueID == op2.UniqueID {
		t.Error("expected different UniqueIDs")
	}
}

// TestPhase1_MakeRSMDoesNotPanic: RSM có thể khởi tạo và shutdown mà không panic.
func TestPhase1_MakeRSMDoesNotPanic(t *testing.T) {
	ts := makeTest(t, -1)
	defer ts.cleanup()
	tester.AnnotateTest("TestPhase1_MakeRSMDoesNotPanic", NSRV)
	ts.Begin("Phase1: startup and shutdown")
}

// TestPhase1_SubmitDoesNotHang: Submit luôn trả kết quả nhanh trong Phase 1
// (chưa block vì chưa implement wait — chỉ trả ErrWrongLeader).
func TestPhase1_SubmitDoesNotHang(t *testing.T) {
	ts := makeTest(t, -1)
	defer ts.cleanup()
	tester.AnnotateTest("TestPhase1_SubmitDoesNotHang", NSRV)
	ts.Begin("Phase1: Submit does not hang")

	done := make(chan struct{})
	go func() {
		for _, s := range ts.srvs {
			s.rsm.Submit(Inc{})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Submit hung — expected it to return quickly in Phase 1")
	}
}

// TestPhase1_ReaderCallsDoOp: reader goroutine chạy và gọi DoOp cho các op đã commit.
// Submit trên leader gọi raft.Start() → Raft commit → reader gọi DoOp → counter tăng.
// Submit vẫn trả ErrWrongLeader (Phase 1), nhưng DoOp vẫn được gọi.
func TestPhase1_ReaderCallsDoOp(t *testing.T) {
	ts := makeTest(t, -1)
	defer ts.cleanup()
	tester.AnnotateTest("TestPhase1_ReaderCallsDoOp", NSRV)
	ts.Begin("Phase1: reader goroutine calls DoOp")

	// Submit liên tục đến tất cả server. Khi gặp leader thì raft.Start() được gọi,
	// Raft commit, reader goroutine trên cả 3 server gọi DoOp → counter tăng.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, s := range ts.srvs {
			s.rsm.Submit(Inc{})
		}
		time.Sleep(50 * time.Millisecond)

		// Kiểm tra tất cả server đã có counter > 0
		allPositive := true
		for _, s := range ts.srvs {
			s.mu.Lock()
			c := s.counter
			s.mu.Unlock()
			if c == 0 {
				allPositive = false
				break
			}
		}
		if allPositive {
			return // reader goroutine đang hoạt động đúng
		}
	}
	t.Fatal("reader goroutine did not call DoOp within 5 seconds")
}
