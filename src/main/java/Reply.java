import org.jboss.netty.buffer.ChannelBuffer;

public class Reply {

  private final RequestId requestId;
  private final int statusCode;
  private final ChannelBuffer payload;

  public Reply(final RequestId requestId, final int statusCode,
               final ChannelBuffer payload) {
    this.statusCode = statusCode;
    this.requestId = requestId;
    this.payload = payload;
  }

  public int getStatusCode() {
    return statusCode;
  }

  public RequestId getRequestId() {
    return requestId;
  }

  public ChannelBuffer getPayload() {
    return payload;
  }

  public static Reply parse(final ChannelBuffer buffer) {
    final RequestId requestId = RequestId.parse(buffer);
    final int statusCode = buffer.readInt();
    final int payloadSize = buffer.readInt();
    return new Reply(requestId, statusCode, buffer.readSlice(payloadSize));
  }

  public int serializedSize() {
    return requestId.serializedSize() +
           4 + // status code
           4 + payload.readableBytes();
  }

  public void serialize(final ChannelBuffer buffer) {
    requestId.serialize(buffer);
    buffer.writeInt(statusCode);
    buffer.writeInt(payload.readableBytes());
    buffer.writeBytes(payload);
  }
}
