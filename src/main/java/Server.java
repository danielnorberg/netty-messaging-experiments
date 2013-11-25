import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executor;

import jsr166.concurrent.atomic.AtomicLong;

public class Server {

  private static final Logger log = LoggerFactory.getLogger(Server.class);

  private final RequestHandler requestHandler;
  private final Channel channel;

  public Server(final InetSocketAddress address, final Executor executor,
                final boolean batching, final RequestHandler requestHandler) {
    this.requestHandler = requestHandler;

    final NioServerSocketChannelFactory channelFactory = new NioServerSocketChannelFactory();
    final ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        final ChannelPipeline pipeline = Channels.pipeline(
            new ReplyEncoder(),

            new MessageFrameDecoder(),
            new ExecutionHandler(executor),
            new RequestDecoder(),
            new Handler());

        if (batching) {
          pipeline.addFirst("batcher", new AutoFlushingWriteBatcher());
        }

        return pipeline;
      }
    });

    this.channel = bootstrap.bind(address);
  }

  static final AtomicLong CHANNEL_ID_COUNTER = new AtomicLong();

  class Handler extends SimpleChannelUpstreamHandler {

    @Override
    public void channelConnected(final ChannelHandlerContext ctx, final ChannelStateEvent e)
        throws Exception {
      e.getChannel().setAttachment(CHANNEL_ID_COUNTER.getAndIncrement());
    }

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
