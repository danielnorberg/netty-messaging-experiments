import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;

import static com.google.common.base.Optional.fromNullable;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.System.out;
import static java.net.InetAddress.getLoopbackAddress;
import static java.util.concurrent.TimeUnit.DAYS;
import static net.sourceforge.argparse4j.impl.Arguments.storeFalse;
import static org.jboss.netty.buffer.ChannelBuffers.EMPTY_BUFFER;

public class Bench {

  public static final int CPUS = Runtime.getRuntime().availableProcessors();

  public static void main(final String... args) throws InterruptedException {
    final ProgressMeter meter = new ProgressMeter();

    final ArgumentParser parser = ArgumentParsers.newArgumentParser("benchmark").defaultHelp(true);

    final String threadsDest = parser.addArgument("-t", "--threads")
        .type(Integer.class).setDefault(CPUS).getDest();
    final String batchingDest = parser.addArgument("--no-batching")
        .type(Boolean.class).action(storeFalse()).getDest();
    final String portDest = parser.addArgument("-p", "--port")
        .type(Integer.class).setDefault(4711).getDest();

    final String connectionsDest = parser.addArgument("-c", "--connections")
        .type(Integer.class).getDest();
    final String outstandingDest = parser.addArgument("-o", "--outstanding")
        .type(Integer.class).getDest();

    final Namespace options;
    try {
      options = parser.parseArgs(args);
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      System.exit(1);
      return;
    }

    final int port = options.getInt(portDest);
    final int threads = options.getInt(threadsDest);
    final boolean batching = options.getBoolean(batchingDest);
    final int connections = fromNullable(options.getInt(connectionsDest)).or(threads);
    final int outstanding = fromNullable(options.getInt(outstandingDest)).or(1000 * connections);

    final InetSocketAddress address = new InetSocketAddress(getLoopbackAddress(), port);

    out.printf("address: %s%n", address);
    out.printf("threads: %s%n", threads);
    out.printf("batching: %s%n", batching);
    out.printf("connections: %s%n", connections);
    out.printf("outstanding: %s%n", outstanding);
    out.println();

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
