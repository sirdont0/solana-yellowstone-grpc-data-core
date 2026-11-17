package core

import (
	"testing"
	"time"
)

func TestHealthExposesOldestPendingDispatchWait(t *testing.T) {
	e, err := NewRaw(configForTest(t))
	if err != nil {
		t.Fatal(err)
	}
	id := e.trackQueueWait()
	time.Sleep(15 * time.Millisecond)
	health := e.Health()["a"]
	if health.QueuePending != 1 || health.QueueOldestWait <= 0 || health.QueueLastWait != 0 {
		t.Fatalf("pending shared dispatch wait not visible: %+v", health)
	}
	e.finishQueueWait(id)
	health = e.Health()["a"]
	if health.QueuePending != 0 || health.QueueOldestWait != 0 || health.QueueLastWait <= 0 || health.QueueMaxWait < health.QueueLastWait {
		t.Fatalf("finished dispatch wait not recorded: %+v", health)
	}
}
