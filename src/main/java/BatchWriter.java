import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
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
  private volatile long lastWriteNanos;
  private volatile int bufferSize;
  private final Deque<ByteBuf> queue = new ConcurrentLinkedDeque<>();
  public volatile long r0, r1, r2, r3, r4, r5, r6, r7;
  public volatile long s0, s1, s2, s3, s4, s5, s6, s7;

  /**
   * Create a new write batcher.
   */
  public BatchWriter(final Channel channel) {
    this.channel = channel;
    this.allocator = channel.alloc();
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