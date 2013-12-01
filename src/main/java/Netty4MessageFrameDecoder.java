import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import static io.netty.buffer.Unpooled.EMPTY_BUFFER;

public class Netty4MessageFrameDecoder extends LengthFieldBasedFrameDecoder {

  public Netty4MessageFrameDecoder() {
    super(128 * 1024 * 1024, 0, 4, 0, 4);
  }

  @Override
  protected ByteBuf extractFrame(final ChannelHandlerContext ctx, final ByteBuf buffer,
                                 final int index, final int length) {
    if (length == 0) {
      return EMPTY_BUFFER;
    }

    // Releasing the slice will release the parent buffer
    buffer.retain();
    return buffer.slice(index, length);
  }
}
