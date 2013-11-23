import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;


public class RequestEncoder extends OneToOneEncoder {

  @Override
  protected Object encode(final ChannelHandlerContext ctx, final Channel channel, final Object msg)
      throws Exception {
    final Request request = (Request) msg;
    final int size = request.serializedSize();
    final ChannelBuffer buffer = ChannelBuffers.buffer(4 + size);
    buffer.writeInt(size);
    request.serialize(buffer);
    return buffer;
  }
}
