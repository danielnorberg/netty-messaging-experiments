import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

import java.net.InetSocketAddress;
import java.util.List;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ReferenceCountUtil;

import static java.lang.System.out;
import static java.net.InetAddress.getLoopbackAddress;
import static java.util.Arrays.asList;

public class SimpleBenchNetty4 {

  static final ByteBuf PAYLOAD = Unpooled.wrappedBuffer(new byte[32]);

  private static ByteBuf duplicate(final ByteBuf buffer) {
    return new UnreleasableReadOnlyByteBuf(buffer);
  }

  static class Server {

    public Server(final InetSocketAddress address) throws InterruptedException {
      final EventLoopGroup workerGroup = new NioEventLoopGroup();

      final ServerBootstrap b = new ServerBootstrap();
      b.group(workerGroup)
          .channel(NioServerSocketChannel.class)
          .childOption(ChannelOption.TCP_NODELAY, true)
          .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(final SocketChannel ch) throws Exception {
              ch.pipeline().addLast(
                  new Netty4MessageFrameDecoder(),
                  new Handler());
            }
          });

      b.bind(address).sync();
    }

    class Handler extends ChannelInboundHandlerAdapter {

      BatchWriter writer;

      @Override
      public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        writer = new BatchWriter(ctx.channel());
      }

      @Override
      public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        try {
          writer.write(duplicate(PAYLOAD));
        } finally {
          ReferenceCountUtil.release(msg);
        }
      }
    }
  }

  static class Client {

    public Client(final List<InetSocketAddress> addresses)
        throws InterruptedException {

      final EventLoopGroup workerGroup = new NioEventLoopGroup();
      for (final InetSocketAddress address : addresses) {
        final Bootstrap b = new Bootstrap();
        b.group(workerGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .handler(new ChannelInitializer<SocketChannel>() {
              @Override
              protected void initChannel(final SocketChannel ch) throws Exception {
                ch.pipeline().addLast(
                    new Netty4MessageFrameDecoder(),
                    new Handler());
              }
            });
        b.connect(address).sync();
      }
    }

    public volatile long p0, p1, p2, p3, p4, p5, p6, p7;
    public volatile long q0, q1, q2, q3, q4, q5, q6, q7;
    private long counter;
    public volatile long r0, r1, r2, r3, r4, r5, r6, r7;
    public volatile long s0, s1, s2, s3, s4, s5, s6, s7;

    private class Handler extends ChannelInboundHandlerAdapter {

      BatchWriter writer;

      @Override
      public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        writer = new BatchWriter(ctx.channel());
        for (int i = 0; i < 1000; i++) {
          send(ctx);
        }
        ctx.flush();
      }

      private void send(final ChannelHandlerContext ctx) {
        writer.write(duplicate(PAYLOAD));
      }

      @Override
      public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        try {
          counter++;
          send(ctx);
          ctx.flush();
        } finally {
          ReferenceCountUtil.release(msg);
        }
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

    final int connections;
    if (args.length > 1) {
      connections = Integer.parseInt(args[1]);
    } else {
      connections = 1;
    }

    final int port;
    if (args.length > 2) {
      port = Integer.parseInt(args[2]);
    } else {
      port = 4711;
    }

    out.printf("instances: %s%n", instances);
    out.printf("connections: %s%n", connections);
    out.printf("port: %s%n", port);

    final List<Client> clients = Lists.newArrayList();

    final List<InetSocketAddress> addresses = Lists.newArrayList();
    for (int i = 0; i < connections; i++) {
      final InetSocketAddress address = new InetSocketAddress(getLoopbackAddress(), port + i);
      final Server server = new Server(address);
      addresses.add(address);
      final Client client = new Client(asList(address));
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
