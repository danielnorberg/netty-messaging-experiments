import org.jboss.netty.buffer.ChannelBuffer;

import java.util.concurrent.atomic.AtomicLong;

public class RequestId {

  private static final AtomicLong ID_COUNTER = new AtomicLong();
  private final long value;

  public RequestId() {
    this(ID_COUNTER.incrementAndGet());
  }

  public RequestId(final long value) {
    this.value = value;
  }

  public long getValue() {
    return value;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final RequestId requestId = (RequestId) o;

    if (value != requestId.value) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return (int) (value ^ (value >>> 32));
  }

  @Override
  public String toString() {
    return String.valueOf(value);
  }

  public static RequestId parse(final ChannelBuffer msg) {
    return new RequestId(msg.readLong());
  }

  public int serializedSize() {
    return 8;
  }

  public void serialize(final ChannelBuffer buffer) {
    buffer.writeLong(value);
  }
}
