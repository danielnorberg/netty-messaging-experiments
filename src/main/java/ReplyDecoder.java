import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;


public class ReplyDecoder extends OneToOneDecoder {

  @Override
  protected Object decode(final ChannelHandlerContext ctx, final Channel channel, final Object msg)
      throws Exception {
    return Reply.parse((ChannelBuffer) msg);
  }
}
