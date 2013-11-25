import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;

public class MessageFrameDecoder extends LengthFieldBasedFrameDecoder {

  public MessageFrameDecoder() {
    super(128 * 1024 * 1024, 0, 4, 0, 4);
  }

  @Override
  protected ChannelBuffer extractFrame(final ChannelBuffer buffer, final int index,
                                       final int length) {
    return buffer.slice(index, length);
  }
}
