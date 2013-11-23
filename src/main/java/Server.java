import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class Server {

  private static final Logger log = LoggerFactory.getLogger(Server.class);

  private final RequestHandler requestHandler;
  private final Channel channel;

  public Server(final InetSocketAddress address, final RequestHandler requestHandler) {
    final InetSocketAddress address1 = address;
    this.requestHandler = requestHandler;

    final NioServerSocketChannelFactory channelFactory = new NioServerSocketChannelFactory();
    final ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        return Channels.pipeline(
            new AutoFlushingWriteBatcher(),
            new ReplyEncoder(),

            new MessageFrameDecoder(),
            new RequestDecoder(),
            new Handler());
      }
    });

    this.channel = bootstrap.bind(address);
  }

  class Handler extends SimpleChannelUpstreamHandler {

    @Override
    public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e)
        throws Exception {
      final Request request = (Request) e.getMessage();
      final RequestContext context = new DefaultRequestContext(ctx.getChannel());
      try {
        requestHandler.handleRequest(request, context);
      } catch (Exception ex) {
        log.error("request handler exception", ex);
      }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final ExceptionEvent e)
        throws Exception {
      log.error("exception", e.getCause());
      ctx.getChannel().close();
    }
  }

  private class DefaultRequestContext implements RequestContext {

    private final Channel channel;

    public DefaultRequestContext(final Channel channel) {
      this.channel = channel;
    }

    @Override
    public void reply(final Reply reply) {
      channel.write(reply);
    }
  }
}
