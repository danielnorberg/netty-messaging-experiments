import com.google.common.collect.Lists;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.queue.BufferedWriteHandler;

import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class AutoFlushingWriteBatcher extends BufferedWriteHandler {

  private static final long MAX_DELAY_NANOS = MICROSECONDS.toNanos(100);
  private static final int MAX_BUFFER_SIZE = 4096;

  private static final AtomicIntegerFieldUpdater<AutoFlushingWriteBatcher> bufferSizeUpdater =
      AtomicIntegerFieldUpdater.newUpdater(AutoFlushingWriteBatcher.class, "bufferSize");

  public volatile long p0, p1, p2, p3, p4, p5, p6, p7;
  public volatile long q0, q1, q2, q3, q4, q5, q6, q7;
  private volatile long lastFlushNanos;
  private volatile long lastWriteNanos;
  private volatile int bufferSize;
  public volatile long r0, r1, r2, r3, r4, r5, r6, r7;
  public volatile long s0, s1, s2, s3, s4, s5, s6, s7;

  // Keeps tracks of all batchers. Assumes that connection churn is low.
  private static final List<AutoFlushingWriteBatcher> batchers = Lists.newCopyOnWriteArrayList();

  // Set up single thread to perform regular flushing of all batchers. This is a lot cheaper than
  // using e.g. a ScheduleThreadPoolExecutor and scheduling each batcher individually.
  static {
    final Thread flusherThread = new Thread(new Runnable() {
      @SuppressWarnings("InfiniteLoopStatement")
      @Override
      public void run() {
        while (true) {
          final long now = System.nanoTime();
          for (final AutoFlushingWriteBatcher batcher : batchers) {
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
  public AutoFlushingWriteBatcher() {
    super(true);
  }

  /**
   * Called when the channel is opened.
   */
  @Override
  public void channelOpen(final ChannelHandlerContext ctx, final ChannelStateEvent e)
      throws Exception {
    super.channelOpen(ctx, e);

    batchers.add(this);
  }

  /**
   * Called when the channel is closed.
   */
  @Override
  public void channelClosed(final ChannelHandlerContext ctx, final ChannelStateEvent e)
      throws Exception {
    super.channelClosed(ctx, e);

    batchers.remove(this);
  }

  /**
   * Called when an outgoing message is written to the channel.
   */
  @Override
  public void writeRequested(final ChannelHandlerContext ctx, final MessageEvent e)
      throws Exception {
    super.writeRequested(ctx, e);

    // Calculate new size of outgoing message buffer
    final ChannelBuffer data = (ChannelBuffer) e.getMessage();
    final int newBufferSize = bufferSizeUpdater.addAndGet(this, data.readableBytes());

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

  @Override
  public void flush() {
    // Record the flush time for use in the scheduled flush task. Do this before flushing
    // to avoid losing writes due to race with write of lastWriteNanos.
    lastFlushNanos = System.nanoTime();

    // The message buffer will become empty. This is racy but we don't care.
    bufferSize = 0;

    super.flush();
  }
}