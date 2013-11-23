import com.google.common.collect.Lists;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.queue.BufferedWriteHandler;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * Copyright (C) 2013 Spotify AB
 */

public class AutoFlushingWriteBatcher extends BufferedWriteHandler {

  private static final long MAX_DELAY_NANOS = MICROSECONDS.toNanos(100);
  private static final int MAX_BUFFER_SIZE = 4096;

  private final AtomicInteger bufferSize = new AtomicInteger();

  private volatile long lastFlushNanos;
  private volatile long lastWriteNanos;

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

          // Sleep a millisecond before going through all batchers again.
          try {
            Thread.sleep(1);
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
    final int newBufferSize = bufferSize.addAndGet(data.readableBytes());

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
    bufferSize.set(0);

    super.flush();
  }
}