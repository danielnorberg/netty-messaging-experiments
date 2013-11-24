import org.jboss.netty.buffer.ChannelBuffer;

import static org.jboss.netty.buffer.ChannelBuffers.EMPTY_BUFFER;

public class Request {

  private final RequestId id;
  private final ChannelBuffer payload;

  public Request(final ChannelBuffer payload) {
    this(RequestId.create(), payload);
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

  public void serialize(final ChannelBuffer buffer) {
    id.serialize(buffer);
    buffer.writeInt(payload.readableBytes());
    buffer.writeBytes(payload);
  }

  public int serializedSize() {
    return id.serializedSize() +
           4 + payload.readableBytes();
  }
}
