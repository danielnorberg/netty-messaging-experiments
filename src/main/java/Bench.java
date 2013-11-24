import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.System.out;
import static java.net.InetAddress.getLoopbackAddress;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.jboss.netty.buffer.ChannelBuffers.EMPTY_BUFFER;

public class Bench {

  public static void main(final String... args) throws InterruptedException {
    final InetSocketAddress address =
        new InetSocketAddress(getLoopbackAddress(), 4711);

    final ProgressMeter meter = new ProgressMeter();

    final int threads;
    final boolean batching;
    final int connections;
    final int outstanding;

    if (args.length > 0) {
      threads = Integer.parseInt(args[0]);
    } else {
      threads = Runtime.getRuntime().availableProcessors();
    }

    if (args.length > 1) {
      batching = Boolean.parseBoolean(args[1]);
    } else {
      batching = true;
    }

    if (args.length > 2) {
      connections = Integer.parseInt(args[2]);
    } else {
      connections = threads;
    }

    if (args.length > 3) {
      outstanding = Integer.parseInt(args[3]);
    } else {
      outstanding = 1000 * connections;
    }

    out.printf("address: %s%n", address);
    out.printf("threads: %s%n", threads);
    out.printf("batching: %s%n", batching);
    out.printf("connections: %s%n", connections);
    out.printf("outstanding: %s%n", outstanding);

    final ExecutorService executor = new ChannelShardedForkJoinPool(threads);

    final Server server = new Server(address, executor, batching, new RequestHandler() {
      @Override
      public void handleRequest(final Request request, final RequestContext context) {
        context.reply(request.makeReply(418));
      }
    });

    final Client client = new Client(address, executor, batching, connections, new ReplyHandler() {
      @Override
      public void handleReply(final Client client, final Reply reply) {
        meter.inc(1, 0);
        client.send(new Request(EMPTY_BUFFER));
      }
    });

    for (int i = 0; i < outstanding; i++) {
      client.send(new Request(EMPTY_BUFFER));
    }

    sleepUninterruptibly(1, DAYS);
  }

  private static ForkJoinPool forkJoinPool(final int threads) {
    return new ForkJoinPool(threads,
                            new ForkJoinPool.ForkJoinWorkerThreadFactory() {
                              @Override
                              public ForkJoinWorkerThread newThread(
                                  final ForkJoinPool pool) {
                                return new WorkerThread(pool);
                              }
                            },
                            new Thread.UncaughtExceptionHandler() {
                              @Override
                              public void uncaughtException(final Thread t,
                                                            final Throwable e) {
                                e.printStackTrace();
                              }
                            }, true
    );
  }

}
