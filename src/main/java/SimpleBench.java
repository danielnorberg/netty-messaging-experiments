import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

import org.jboss.netty.bootstrap.ClientBootstrap;
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

    public Client(final InetSocketAddress address)
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
              new Handler()
          );
        }
      });

      bootstrap.connect(address);
    }

    public volatile long p0, p1, p2, p3, p4, p5, p6, p7;
    public volatile long q0, q1, q2, q3, q4, q5, q6, q7;
    private long counter;
    public volatile long r0, r1, r2, r3, r4, r5, r6, r7;
    public volatile long s0, s1, s2, s3, s4, s5, s6, s7;

    private class Handler extends SimpleChannelUpstreamHandler {

      @Override
      public void channelConnected(final ChannelHandlerContext ctx, final ChannelStateEvent e)
          throws Exception {
        final Channel channel = e.getChannel();
        for (int i = 0; i < 1000; i++) {
          send(channel);
        }
      }

      private void send(final Channel channel) {
        final RequestId requestId = new RequestId(counter, 0);
        final Request request = new Request(requestId, EMPTY_BUFFER);
        channel.write(request);
      }

      @Override
      public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e)
          throws Exception {
        counter++;
        send(e.getChannel());
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

    final List<Client> clients = Lists.newArrayList();

    for (int i = 0; i < instances; i++) {
      final InetSocketAddress address = new InetSocketAddress(getLoopbackAddress(), 4711 + i);
      final Server server = new Server(address);
      final Client client = new Client(address);
      clients.add(client);
    }

    final ProgressMeter meter = new ProgressMeter(new Supplier<ProgressMeter.Counters>() {
      @Override
      public ProgressMeter.Counters get() {
        long requests = 0;
        for (final Client client : clients) {
          requests += client.counter;
        }
        return new ProgressMeter.Counters(requests, 0);
      }
    });
  }
}
