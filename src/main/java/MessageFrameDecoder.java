import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;

public class MessageFrameDecoder extends LengthFieldBasedFrameDecoder {

  public MessageFrameDecoder() {
    super(128 * 1024 * 1024, 0, 4, 0, 4);
  }
}
