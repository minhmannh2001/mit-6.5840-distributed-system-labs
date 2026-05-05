package rsm

import (
	"sync"
	"testing"

	tester "6.5840/tester1"
)

// TestPhase2_SubmitReturnsOK: Submit trên leader phải trả rpc.OK và kết quả đúng.
func TestPhase2_SubmitReturnsOK(t *testing.T) {
	ts := makeTest(t, -1)
	defer ts.cleanup()
	tester.AnnotateTest("TestPhase2_SubmitReturnsOK", NSRV)
	ts.Begin("Phase2: Submit returns OK with result")

	r := ts.oneInc()
	if r == nil {
		t.Fatal("Submit trả nil — Submit chưa trả rpc.OK trên leader")
	}
	if r.N != 1 {
		t.Fatalf("expected N=1, got N=%d", r.N)
	}
	ts.checkCounter(1, NSRV)
}

// TestPhase2_SubmitSequential: submit liên tiếp, counter tăng đúng thứ tự.
func TestPhase2_SubmitSequential(t *testing.T) {
	const N = 5
	ts := makeTest(t, -1)
	defer ts.cleanup()
	tester.AnnotateTest("TestPhase2_SubmitSequential", NSRV)
	ts.Begin("Phase2: sequential submits return incrementing results")

	for i := 1; i <= N; i++ {
		r := ts.oneInc()
		if r == nil {
			t.Fatalf("oneInc trả nil tại i=%d", i)
		}
		if r.N != i {
			t.Fatalf("expected N=%d, got N=%d", i, r.N)
		}
	}
	ts.checkCounter(N, NSRV)
}

// TestPhase2_SubmitConcurrent: submit song song, tất cả phải hoàn thành.
func TestPhase2_SubmitConcurrent(t *testing.T) {
	const N = 20
	ts := makeTest(t, -1)
	defer ts.cleanup()
	tester.AnnotateTest("TestPhase2_SubmitConcurrent", NSRV)
	ts.Begin("Phase2: concurrent submits all complete")

	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ts.oneInc()
		}()
	}
	wg.Wait()
	ts.checkCounter(N, NSRV)
}
