import org.jboss.netty.buffer.ChannelBuffer;

import java.util.concurrent.atomic.AtomicLong;

public class RequestId {

  private static final AtomicLong ID_COUNTER = new AtomicLong();
  private final long value;
  private final long timestampMillis;

  public RequestId() {
    this(ID_COUNTER.incrementAndGet(), System.currentTimeMillis());
  }

  public RequestId(final long value, final long timestampMillis) {
    this.value = value;
    this.timestampMillis = timestampMillis;
  }

  public long getValue() {
    return value;
  }

  public long getTimestampMillis() {
    return timestampMillis;
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

    if (timestampMillis != requestId.timestampMillis) {
      return false;
    }
    if (value != requestId.value) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (value ^ (value >>> 32));
    result = 31 * result + (int) (timestampMillis ^ (timestampMillis >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return String.valueOf(value);
  }

  public static RequestId parse(final ChannelBuffer msg) {
    return new RequestId(msg.readLong(), msg.readLong());
  }

  public int serializedSize() {
    return 8 + 8;
  }

  public void serialize(final ChannelBuffer buffer) {
    buffer.writeLong(value);
    buffer.writeLong(timestampMillis);
  }
}
