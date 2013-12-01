import org.jboss.netty.buffer.ChannelBuffer;

import static org.jboss.netty.buffer.ChannelBuffers.EMPTY_BUFFER;

public class Request implements Message {

  private final RequestId id;
  private final ChannelBuffer payload;

  @Deprecated
  public Request(final ChannelBuffer payload) {
    this(RequestId.create(), payload);
  }

  public Request(final long id, final ChannelBuffer payload) {
    this(new RequestId(id, System.currentTimeMillis()), payload);
  }

  public Request(final RequestId id, final ChannelBuffer payload) {
    this.id = id;
    this.payload = payload;
  }

  public Reply makeReply(final int statusCode) {
    return makeReply(statusCode, EMPTY_BUFFER);
  }

  public Reply makeReply(final int statusCode, final ChannelBuffer payload) {
    return new Reply(id, statusCode, payload);
  }

  public RequestId getId() {
    return id;
  }

  public ChannelBuffer getPayload() {
    return payload;
  }

  public static Request parse(final ChannelBuffer buffer) {
    final RequestId requestId = RequestId.parse(buffer);
    final int payloadSize = buffer.readInt();
    final ChannelBuffer payload = buffer.readSlice(payloadSize);
    return new Request(requestId, payload);
  }

  @Override
  public void serialize(final ChannelBuffer buffer) {
    id.serialize(buffer);
    buffer.writeInt(payload.readableBytes());
    buffer.writeBytes(payload);
  }

  @Override
  public int serializedSize() {
    return id.serializedSize() +
           4 + payload.readableBytes();
  }
}
