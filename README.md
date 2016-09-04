# waitqueue

A simple wait queue that allows you to queue something as a producer and wait till some consumer has finished processing it.

The design is completely threadsafe but it might be CPU heavy when the queue is often filled.
