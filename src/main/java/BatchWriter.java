import com.google.common.collect.Lists;

import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class BatchWriter {

  private static final long MAX_DELAY_NANOS = MICROSECONDS.toNanos(100);
  private static final int MAX_BUFFER_SIZE = 4096;

  private static final AtomicIntegerFieldUpdater<BatchWriter> bufferSizeUpdater =
      AtomicIntegerFieldUpdater.newUpdater(BatchWriter.class, "bufferSize");
  private static final int HEADER_SIZE = 4;

  private final Channel channel;
  private final ByteBufAllocator allocator;

  private final AtomicBoolean flushing = new AtomicBoolean();

  public volatile long p0, p1, p2, p3, p4, p5, p6, p7;
  public volatile long q0, q1, q2, q3, q4, q5, q6, q7;
  private volatile long lastFlushNanos;
  private volatile long lastWriteNanos;
  private volatile int bufferSize;
  private volatile int bufferCount;
  private final Deque<ByteBuf> queue = new ConcurrentLinkedDeque<>();
  public volatile long r0, r1, r2, r3, r4, r5, r6, r7;
  public volatile long s0, s1, s2, s3, s4, s5, s6, s7;

  // Keeps tracks of all batchers. Assumes that connection churn is low.
  private static final List<BatchWriter> batchers = Lists.newCopyOnWriteArrayList();

  // Set up single thread to perform regular flushing of all batchers. This is a lot cheaper than
  // using e.g. a ScheduleThreadPoolExecutor and scheduling each batcher individually.
  static {
    final Thread flusherThread = new Thread(new Runnable() {
      @SuppressWarnings("InfiniteLoopStatement")
      @Override
      public void run() {
        while (true) {
          final long now = System.nanoTime();
          for (final BatchWriter batcher : batchers) {
            final long lastFlushNanos = batcher.lastFlushNanos;
            final long nanosSinceLastFlush = now - lastFlushNanos;

            // Only call flush if needed, as it does an expensive cas
            if (nanosSinceLastFlush > MAX_DELAY_NANOS &&
                batcher.lastWriteNanos > lastFlushNanos) {
              batcher.flush();
            }
          }

          // Sleep ten milliseconds before going through all batchers again.
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            // ignore
          }
        }
      }
    });

    flusherThread.setName("buffer-flusher");
    flusherThread.setDaemon(true);
    flusherThread.start();
  }

  /**
   * Create a new write batcher.
   */
  public BatchWriter(final Channel channel) {
    this.channel = channel;
    this.allocator = channel.alloc();
    batchers.add(this);
  }

  public void write(final ByteBuf message) {
    queue.add(message);

    // Calculate new size of outgoing message buffer
    final int frameSize = message.readableBytes() + HEADER_SIZE;
    final int newBufferSize = bufferSizeUpdater.addAndGet(this, frameSize);

    // Calculate how long it was since the last outgoing message
    final long now = System.nanoTime();
    final long nanosSinceLastWrite = now - lastWriteNanos;
    lastWriteNanos = now;

    // Flush if writes are sparse or if the buffer has reached its threshold size
    if (nanosSinceLastWrite > MAX_DELAY_NANOS ||
        newBufferSize > MAX_BUFFER_SIZE) {
      flush();
    }
  }

  private void flush() {
    // Record the flush time for use in the scheduled flush task. Do this before flushing
    // to avoid losing writes due to race with write of lastWriteNanos.
    lastFlushNanos = System.nanoTime();

    if (!flushing.compareAndSet(false, true)) {
      return;
    }

    // The message buffer will become empty. This is racy but we don't care.
    final int bufferSize = this.bufferSize;
    this.bufferSize = 0;

    final ByteBuf buffer = allocator.buffer(bufferSize);
    long n = 0;

    while (true) {
      final ByteBuf message = queue.poll();
      if (message == null) {
        break;
      }
      final int size = message.readableBytes();
      if (n + HEADER_SIZE + size > bufferSize) {
        queue.addFirst(message);
        break;
      }
      buffer.writeInt(size);
      buffer.writeBytes(message);
      message.release();
    }

    channel.writeAndFlush(buffer);

    flushing.set(false);
  }
}