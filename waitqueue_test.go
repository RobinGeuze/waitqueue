package waitqueue

import (
	"testing"
	_ "net/http/pprof"
)

func TestWaitQueue(t *testing.T) {
	consumedChannel := make(chan bool)
	testQueue, err := NewRingQueue(256)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		for i := 0; i < 1000000; i++ {
			someValue := 1
			testQueue.Enqueue(someValue)
		}
		consumedChannel <- true
	}()

	go func() {
		for i := 0; i < 1000000; i++ {
			value, compFunc := testQueue.Dequeue()
			if value != 1 {
				t.Fatalf("Wrong value passed through, %d instead of %d", value, 1)
			}
			compFunc()
		}
	}()

	<- consumedChannel
}