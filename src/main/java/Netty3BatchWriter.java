import com.google.common.collect.Lists;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.socket.nio.NioSocketChannel;

import java.util.List;

public class Netty3BatchWriter {

  private static final int MAX_BUFFER_SIZE = 4096;
  private static final int HEADER_SIZE = 4;

  private volatile long p0, p1, p2, p3, p4, p5, p6, p7;
  private volatile long q0, q1, q2, q3, q4, q5, q6, q7;

  private final NioSocketChannel channel;
  private final List<ChannelBuffer> queue = Lists.newArrayList();
  private int bufferSize;
  private boolean registered;
  private final Runnable flusher = new Runnable() {
    @Override
    public void run() {
      registered = false;
      flush();
    }
  };

  private volatile long r0, r1, r2, r3, r4, r5, r6, r7;
  private volatile long s0, s1, s2, s3, s4, s5, s6, s7;


  /**
   * Create a new write batcher.
   */
  public Netty3BatchWriter(final NioSocketChannel channel) {
    this.channel = channel;
  }

  public void write(final ChannelBuffer message) {
    queue.add(message);

    // Calculate new size of outgoing message buffer
    final int frameSize = message.readableBytes() + HEADER_SIZE;
    bufferSize += frameSize;

    // Flush if the buffer has reached its threshold size
    if (bufferSize > MAX_BUFFER_SIZE) {
      flush();
    }

    if (!registered) {
      registered = true;
      channel.getWorker().executeInIoThread(flusher, true);
    }
  }

  public void flush() {
    if (bufferSize == 0) {
      return;
    }

    final ChannelBuffer buffer = ChannelBuffers.buffer(bufferSize);

    for (int i = 0; i < queue.size(); i++) {
      final ChannelBuffer message = queue.get(i);
      final int size = message.readableBytes();
      buffer.writeInt(size);
      buffer.writeBytes(message);
    }

    channel.write(buffer);

    queue.clear();
    bufferSize = 0;
  }
}