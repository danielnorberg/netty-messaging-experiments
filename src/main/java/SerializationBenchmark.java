import com.google.common.collect.Lists;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import java.util.List;
import java.util.concurrent.Executor;

import jsr166.concurrent.Executors;

/**
 * Copyright (C) 2013 Spotify AB
 */

public class SerializationBenchmark  {

  private static final int CPUS = Runtime.getRuntime().availableProcessors();

  public static void main(final String... args) throws InterruptedException {
    final Executor executor = Executors.newCachedThreadPool();
    final List<Worker> workers = Lists.newArrayList();
    for (int i = 0; i < CPUS; i++) {
      final Worker worker = new Worker();
      workers.add(worker);
      executor.execute(worker);
    }

    long bytes = 0;
    long messages = 0;
    long parsedBytes = 0;
    while (true) {
      long newBytes = 0;
      long newMessages = 0;
      long newParsedBytes = 0;
      for (final Worker worker : workers) {
        newBytes += worker.bytes;
        newMessages += worker.messages;
        newParsedBytes += worker.parsedBytes;
      }
      long deltaBytes = newBytes - bytes;
      long deltaMessages = newMessages - messages;
      long deltaParsedBytes = newParsedBytes - parsedBytes;
      bytes = newBytes;
      messages = newMessages;
      parsedBytes = newParsedBytes;
      System.out.printf("%,d b/s, %,d m/s, %,d pb/s%n", deltaBytes, deltaMessages, deltaParsedBytes);
      Thread.sleep(1000);
    }
  }

  private static class Worker implements Runnable {

    private static final int N = 1000;

    private volatile long p0, p1, p2, p3, p4, p5, p6, p7;
    private volatile long q0, q1, q2, q3, q4, q5, q6, q7;

    private final Request request;
//    private final ChannelBuffer buffer;
    private volatile long bytes;
    private volatile long messages;
    private volatile long parsedBytes;

    private volatile long r0, r1, r2, r3, r4, r5, r6, r7;
    private volatile long s0, s1, s2, s3, s4, s5, s6, s7;

    private Worker() {
      request = new Request(new RequestId(0, 0), ChannelBuffers.EMPTY_BUFFER);
    }

    @Override
    public void run() {
      while (true) {
        doRun();
      }
    }

    private void doRun() {
      final ChannelBuffer buffer = ChannelBuffers.buffer(N * request.serializedSize());
      buffer.resetWriterIndex();
      long parsedBytes = 0;
      for (int i = 0; i < N; i++) {
        request.serialize(buffer);
      }
      this.bytes += buffer.readableBytes();
      for (int i = 0; i < N; i++) {
        final Request parsed = Request.parse(buffer);
        parsedBytes += parsed.serializedSize();
      }
      this.messages += N;
      this.parsedBytes += parsedBytes;
    }
  }
}
