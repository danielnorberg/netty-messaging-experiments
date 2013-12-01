import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;

public class MessageFrameEncoder extends LengthFieldPrepender {

  public MessageFrameEncoder() {
    super(4);
  }
}
