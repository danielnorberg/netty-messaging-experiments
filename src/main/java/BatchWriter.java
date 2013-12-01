import com.google.common.collect.Lists;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;

public class BatchWriter {

  private static final int MAX_BUFFER_SIZE = 4096;

  private static final int HEADER_SIZE = 4;

  private final Channel channel;
  private final ByteBufAllocator allocator;

  public volatile long p0, p1, p2, p3, p4, p5, p6, p7;
  public volatile long q0, q1, q2, q3, q4, q5, q6, q7;
  private final List<ByteBuf> queue = Lists.newArrayList();
  private int bufferSize;
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
    bufferSize += frameSize;

    // Flush if the buffer has reached its threshold size
    if (bufferSize > MAX_BUFFER_SIZE) {
      flush();
    }
  }

  public void flush() {
    final ByteBuf buffer = allocator.buffer(bufferSize);

    for (int i = 0; i < queue.size(); i++) {
      final ByteBuf message = queue.get(i);
      final int size = message.readableBytes();
      buffer.writeInt(size);
      buffer.writeBytes(message);
      message.release();
    }

    channel.writeAndFlush(buffer);

    queue.clear();
    bufferSize = 0;
  }
}