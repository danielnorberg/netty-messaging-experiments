import java.net.InetSocketAddress;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.net.InetAddress.getLoopbackAddress;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.jboss.netty.buffer.ChannelBuffers.EMPTY_BUFFER;

public class Bench {

  public static void main(final String... args) throws InterruptedException {
    InetSocketAddress address =
        new InetSocketAddress(getLoopbackAddress(), 4711);

    final ProgressMeter meter = new ProgressMeter();

    final Server server = new Server(address, new RequestHandler() {
      @Override
      public void handleRequest(final Request request, final RequestContext context) {
        context.reply(request.makeReply(418));
      }
    });

    final Client client = new Client(address);

    final int n = 1000;

    for (int i = 0; i < n; i++) {
      send(client, meter);
    }

    sleepUninterruptibly(1, DAYS);
  }

  private static void send(final Client client, final ProgressMeter meter) {
    client.send(new Request(EMPTY_BUFFER), new ReplyHandler() {
      @Override
      public void handleReply(final Reply reply) {
        meter.inc(1, 0);
        send(client, meter);
      }
    });
  }
}
