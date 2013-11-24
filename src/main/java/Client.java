import com.google.common.collect.Lists;

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
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;

public class Client {

  private static final Logger log = LoggerFactory.getLogger(Client.class);

  private final List<Channel> channels = Lists.newArrayList();
  private final ReplyHandler replyHandler;

  public Client(final InetSocketAddress address, final Executor executor, final boolean batching,
                final int connections, final ReplyHandler replyHandler)
      throws InterruptedException {
    this.replyHandler = replyHandler;
    final ClientSocketChannelFactory channelFactory = new NioClientSocketChannelFactory();
    final ClientBootstrap bootstrap = new ClientBootstrap(channelFactory);
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        final ChannelPipeline pipeline = Channels.pipeline(
            new RequestEncoder(),

            new MessageFrameDecoder(),
            new ExecutionHandler(executor),
            new ReplyDecoder(),
            new Handler()
        );

        if (batching) {
          pipeline.addFirst("batcher", new AutoFlushingWriteBatcher());
        }

        return pipeline;
      }
    });

    for (int i = 0; i < connections; i++) {
      final Channel channel = bootstrap.connect(address).await().getChannel();
      channels.add(channel);
    }
  }

  public void send(final Request request) {
    final Thread thread = Thread.currentThread();
    final int index;
    if (thread instanceof WorkerThread) {
      final long r = ((WorkerThread) thread).getIndex();
      index = (int) (r % channels.size());
    } else {
      index = ThreadLocalRandom.current().nextInt(0, channels.size());
    }
    final Channel channel = channels.get(index);
    channel.write(request);
  }


  private class Handler extends SimpleChannelUpstreamHandler {

    @Override
    public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e)
        throws Exception {
      final Reply reply = (Reply) e.getMessage();
      try {
        replyHandler.handleReply(Client.this, reply);
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
