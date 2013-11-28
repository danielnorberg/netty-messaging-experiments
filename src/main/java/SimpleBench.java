import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

import com.netflix.hystrix.util.LongAdder;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.List;

import jsr166.concurrent.Executors;

import static java.net.InetAddress.getLoopbackAddress;
import static org.jboss.netty.buffer.ChannelBuffers.EMPTY_BUFFER;

public class SimpleBench {

  static class Server {

    public Server(final InetSocketAddress address) {
      final NioServerSocketChannelFactory channelFactory = new NioServerSocketChannelFactory(
          Executors.newCachedThreadPool(), Executors.newCachedThreadPool(), 1);

      final ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);
      bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
        @Override
        public ChannelPipeline getPipeline() throws Exception {
          return Channels.pipeline(new AutoFlushingWriteBatcher(),
                                   new ReplyEncoder(),

                                   new MessageFrameDecoder(),
                                   new RequestDecoder(),
                                   new Handler());
        }
      });

      bootstrap.bind(address);
    }

    class Handler extends SimpleChannelUpstreamHandler {

      @Override
      public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e)
          throws Exception {
        final Request request = (Request) e.getMessage();
        final Reply reply = request.makeReply(418);
        e.getChannel().write(reply);
      }

      @Override
      public void exceptionCaught(final ChannelHandlerContext ctx, final ExceptionEvent e)
          throws Exception {
        e.getCause().printStackTrace();
        e.getChannel().close();
      }
    }
  }

  static class Client {

    public Client(final InetSocketAddress address, final LongAdder counter)
        throws InterruptedException {
      final ClientSocketChannelFactory channelFactory = new NioClientSocketChannelFactory(
          Executors.newCachedThreadPool(), Executors.newCachedThreadPool(), 1);

      final ClientBootstrap bootstrap = new ClientBootstrap(channelFactory);
      bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
        @Override
        public ChannelPipeline getPipeline() throws Exception {
          return Channels.pipeline(
              new AutoFlushingWriteBatcher(),
              new RequestEncoder(),

              new MessageFrameDecoder(),
              new ReplyDecoder(),
              new Handler(counter)
          );
        }
      });

      bootstrap.connect(address);
    }

    private class Handler extends SimpleChannelUpstreamHandler {

      private final LongAdder counter;

      public Handler(final LongAdder counter) {
        this.counter = counter;
      }

      @Override
      public void channelConnected(final ChannelHandlerContext ctx, final ChannelStateEvent e)
          throws Exception {
        for (int i = 0; i < 1000; i++) {
          e.getChannel().write(new Request(EMPTY_BUFFER));
        }
      }

      @Override
      public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e)
          throws Exception {
        counter.increment();
        e.getChannel().write(new Request(EMPTY_BUFFER));
      }

      @Override
      public void exceptionCaught(final ChannelHandlerContext ctx, final ExceptionEvent e)
          throws Exception {
        e.getCause().printStackTrace();
        ctx.getChannel().close();
      }
    }
  }

  public static void main(final String... args) throws InterruptedException {
    final int instances;
    if (args.length > 0) {
      instances = Integer.parseInt(args[0]);
    } else {
      instances = 1;
    }

    final List<LongAdder> counters = Lists.newArrayList();

    for (int i = 0; i < instances; i++) {
      final LongAdder counter = new LongAdder();
      final InetSocketAddress address = new InetSocketAddress(getLoopbackAddress(), 4711 + i);
      final Server server = new Server(address);
      final Client client = new Client(address, counter);
      counters.add(counter);
    }

    final ProgressMeter meter = new ProgressMeter(new Supplier<ProgressMeter.Counters>() {
      @Override
      public ProgressMeter.Counters get() {
        long requests = 0;
        for (final LongAdder counter : counters) {
          requests += counter.longValue();
        }
        return new ProgressMeter.Counters(requests, 0);
      }
    });
  }
}
