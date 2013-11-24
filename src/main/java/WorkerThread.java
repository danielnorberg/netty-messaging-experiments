/**
 * Copyright (C) 2013 Spotify AB
 */

import java.security.SecureRandom;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;

class WorkerThread extends ForkJoinWorkerThread {

  private static long randomLong(long x) {
    x ^= (x << 21);
    x ^= (x >>> 35);
    x ^= (x << 4);
    return x;
  }

  private long rand = new SecureRandom().nextInt();

  public long random() {
    rand = randomLong(rand);
    return rand;
  }

  public WorkerThread(final ForkJoinPool pool) {
    super(pool);
  }
}
