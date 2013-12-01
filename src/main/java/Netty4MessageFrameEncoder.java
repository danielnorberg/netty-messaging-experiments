import io.netty.handler.codec.LengthFieldPrepender;

public class Netty4MessageFrameEncoder extends LengthFieldPrepender {

  public Netty4MessageFrameEncoder() {
    super(4);
  }
}
