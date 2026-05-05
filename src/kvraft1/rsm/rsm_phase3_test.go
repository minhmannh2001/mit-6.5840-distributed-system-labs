package rsm

import (
	"sync"
	"testing"
	"time"

	"6.5840/kvsrv1/rpc"
	tester "6.5840/tester1"
)

// TestPhase3_PartitionedLeaderSubmitReturns: Submit vào leader bị partition phải
// block trong khi bị cô lập, và trả ErrWrongLeader sau khi partition được heal.
// Pattern giống TestLeaderPartition4A: force election bằng cách submit vào majority.
func TestPhase3_PartitionedLeaderSubmitReturns(t *testing.T) {
	const NSUB = 5

	ts := makeTest(t, -1)
	defer ts.cleanup()
	tester.AnnotateTest("TestPhase3_PartitionedLeaderSubmitReturns", NSRV)
	ts.Begin("Phase3: Submit to partitioned leader returns after reconnect")

	ts.oneInc()

	foundl, l := Leader(ts.Config, Gid)
	if !foundl {
		t.Fatal("no leader found")
	}
	// p1 = majority (n/2+1 servers), p2 = minority (old leader only)
	p1, p2 := ts.Group(Gid).MakePartition(l)
	ts.Group(Gid).Partition(p1, p2)

	// Submit vào minority leader — phải block trong khi bị cô lập
	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < NSUB; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err, _ := ts.srvs[l].rsm.Submit(Null{})
				if err == rpc.OK {
					t.Errorf("Submit to minority leader should not return OK")
				}
			}()
		}
		wg.Wait()
		close(done)
	}()

	// Cho old leader đủ thời gian để Submit goroutines bắt đầu
	time.Sleep(10 * time.Millisecond)

	// Force election trong majority: submit Inc vào p1
	// Đảm bảo majority bầu leader mới và commit, nâng term cao hơn old leader
	ts.onePartition(p1, Inc{})

	// Sau khi majority commit, Submits trên minority vẫn phải block
	select {
	case <-done:
		t.Fatal("Submits to minority completed too soon — should block while partitioned")
	case <-time.After(time.Second):
	}

	// Heal partition
	ts.Group(Gid).ConnectAll()

	// Sau heal, tất cả Submit phải trả về trong vòng 2 giây
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Submits to partitioned leader did not return after partition healed")
	}
}
