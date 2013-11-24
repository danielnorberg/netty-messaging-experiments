/**
 * Copyright (C) 2013 Spotify AB
 */

import java.security.SecureRandom;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicInteger;

class WorkerThread extends ForkJoinWorkerThread {

  private static final AtomicInteger INDEX_COUNTER = new AtomicInteger();

  private final int index;

  private static long randomLong(long x) {
    x ^= (x << 21);
    x ^= (x >>> 35);
    x ^= (x << 4);
    return x;
  }

  private long rand = new SecureRandom().nextInt();

  public WorkerThread(final ForkJoinPool pool) {
    super(pool);
    this.index = INDEX_COUNTER.getAndIncrement();
  }

  public long random() {
    rand = randomLong(rand);
    return rand;
  }

  int getIndex() {
    return index;
  }
}
