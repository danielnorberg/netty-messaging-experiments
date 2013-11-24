import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.System.out;
import static java.net.InetAddress.getLoopbackAddress;
import static java.util.concurrent.TimeUnit.DAYS;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;
import static org.jboss.netty.buffer.ChannelBuffers.EMPTY_BUFFER;

public class Bench {

  public static final int CPUS = Runtime.getRuntime().availableProcessors();

  public static void main(final String... args) throws InterruptedException {
    final InetSocketAddress address =
        new InetSocketAddress(getLoopbackAddress(), 4711);

    final ProgressMeter meter = new ProgressMeter();

    final ArgumentParser parser = ArgumentParsers.newArgumentParser("benchmark").defaultHelp(true);

    final String threadsDest = parser.addArgument("-t", "--threads")
        .type(Integer.class).setDefault(CPUS).getDest();
    final String batchingDest = parser.addArgument("-b", "--batching")
        .action(storeTrue()).getDest();
    final String connectionsDest = parser.addArgument("-c", "--connections")
        .setDefault(CPUS).getDest();
    final String portDest = parser.addArgument("-p", "--port")
        .setDefault(4711).getDest();
    final String outstandingDest = parser.addArgument("-o", "--outstanding")
        .setDefault(1000).getDest();

    final Namespace options;
    try {
      options = parser.parseArgs(args);
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      System.exit(1);
      return;
    }

    final int threads = options.getInt(threadsDest);
    final boolean batching = options.getBoolean(batchingDest);
    final int connections = options.getInt(connectionsDest);
    final int outstanding = options.getInt(outstandingDest);

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
