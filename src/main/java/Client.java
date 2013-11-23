import com.google.common.collect.Maps;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentMap;

public class Client {

  private static final Logger log = LoggerFactory.getLogger(Client.class);

  private final Channel channel;
  private final ConcurrentMap<RequestId, ReplyHandler> outstanding = Maps.newConcurrentMap();

  public Client(final InetSocketAddress address) throws InterruptedException {
    final ClientSocketChannelFactory channelFactory = new NioClientSocketChannelFactory();
    final ClientBootstrap bootstrap = new ClientBootstrap(channelFactory);
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        return Channels.pipeline(
            new AutoFlushingWriteBatcher(),
            new RequestEncoder(),

            new MessageFrameDecoder(),
            new ReplyDecoder(),
            new Handler()
        );
      }
    });

    this.channel = bootstrap.connect(address).await().getChannel();
  }

  public void send(final Request request, final ReplyHandler replyHandler) {
    outstanding.put(request.getId(), replyHandler);
    channel.write(request);
  }


  private class Handler extends SimpleChannelUpstreamHandler {

    @Override
    public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e)
        throws Exception {
      final Reply reply = (Reply) e.getMessage();
      final ReplyHandler replyHandler = outstanding.remove(reply.getRequestId());
      if (replyHandler == null) {
        log.warn("received spurious reply: {}", reply);
        return;
      }
      try {
        replyHandler.handleReply(reply);
      } catch (Exception ex) {
        log.error("reply handler exception", ex);
      }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final ExceptionEvent e)
        throws Exception {
      log.error("exception", e.getCause());
      ctx.getChannel().close();
    }
  }
}
