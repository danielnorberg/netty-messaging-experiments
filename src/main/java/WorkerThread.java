import java.security.SecureRandom;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicInteger;

class WorkerThread extends ForkJoinWorkerThread {

  private static final AtomicInteger INDEX_COUNTER = new AtomicInteger();

  private volatile long p0, p1, p2, p3, p4, p5, p6;
  private final int index;
  private long rand = new SecureRandom().nextInt();
  private long requestCounter;
  private long latencyCounter;
  private volatile long q0, q1, q2, q3, q4, q5, q6;

  public WorkerThread(final ForkJoinPool pool) {
    super(pool);
    this.index = INDEX_COUNTER.getAndIncrement();
  }

  private static long randomLong(long x) {
    x ^= (x << 21);
    x ^= (x >>> 35);
    x ^= (x << 4);
    return x;
  }

  public long random() {
    rand = randomLong(rand);
    return rand;
  }

  public int getIndex() {
    return index;
  }

  public void incRequestCounter(final long latency) {
    requestCounter += 1;
    latencyCounter += latency;
  }

  long getTotalRequests() {
    return requestCounter;
  }

  long getTotalLatency() {
    return latencyCounter;
  }
}
