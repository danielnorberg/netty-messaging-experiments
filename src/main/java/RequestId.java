import org.jboss.netty.buffer.ChannelBuffer;

import jsr166.concurrent.atomic.AtomicLong;

public class RequestId {

  private static final AtomicLong ANONYMOUS_ID_COUNTER = new AtomicLong();
  private final long value;
  private final long timestampMillis;

  @Deprecated
  public static RequestId create() {
    final Thread thread = Thread.currentThread();
    final long id;
    if (thread instanceof WorkerThread) {
      id = ((WorkerThread) thread).nextRequestId();
    } else {
      id = ANONYMOUS_ID_COUNTER.getAndIncrement();
    }

    return new RequestId(id, System.currentTimeMillis());

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
    return value + "@" + timestampMillis;
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
