import com.google.common.base.Supplier;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.AbstractService;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WaitStrategy;

import static com.lmax.disruptor.RingBuffer.createSingleProducer;
import static java.lang.System.out;

public class DisruptorPrimitiveSingleServerClientSplitReactorReqRepBench3 {

  private static class ReplyEvent {

    public static final EventFactory<ReplyEvent> FACTORY = new Factory();

    long clientId;
    long id;

    static class Factory implements EventFactory<ReplyEvent> {

      @Override
      public ReplyEvent newInstance() {
        return new ReplyEvent();
      }
    }
  }

  static class RequestEvent {

    public static final EventFactory<RequestEvent> FACTORY = new Factory();

    long clientId;
    long id;

    static class Factory implements EventFactory<RequestEvent> {

      @Override
      public RequestEvent newInstance() {
        return new RequestEvent();
      }
    }
  }

  static class Server extends AbstractExecutionThreadService {

    private final RingBuffer<RequestEvent> requests;
    private final RingBuffer<ReplyEvent> replies;
    private final int batchSize;
    private final SequenceBarrier barrier;
    private final Sequence sequence = new Sequence();

    Server(final RingBuffer<RequestEvent> requests, final RingBuffer<ReplyEvent> replies,
           final int batchSize) {
      this.requests = requests;
      this.replies = replies;
      this.batchSize = batchSize;
      this.barrier = requests.newBarrier();
      requests.addGatingSequences(sequence);
    }

    @SuppressWarnings({"InfiniteLoopStatement", "LoopStatementThatDoesntLoop"})
    @Override
    public void run() {
      while (true) {
        try {
          while (true) {
            process();
          }
        } catch (InterruptedException | AlertException e) {
          e.printStackTrace();
          return;
        } catch (TimeoutException ignore) {
        }
      }
    }

    private void process() throws InterruptedException, TimeoutException, AlertException {
      final long last = sequence.get();
      final long lo = last + 1;
      final long hi = barrier.waitFor(lo);
      final int count = (int) (hi - last);
      final long replyHi = replies.next(count);
      final long replyLo = replyHi - count + 1;
      int n = 0;
      for (long i = 0; i < count; i++, n++) {
        final long seq = lo + i;
        final long replySeq = replyLo + i;
        final RequestEvent requestEvent = requests.get(seq);
        final ReplyEvent replyEvent = replies.get(replySeq);
        replyEvent.clientId = requestEvent.clientId;
        replyEvent.id = requestEvent.id;
        if (n == batchSize) {
          n = 0;
          replies.publish(seq);
          sequence.set(replySeq);
        }
      }
      replies.publish(replyHi);
      sequence.set(hi);
    }
  }

  static class Client extends AbstractExecutionThreadService {

    private final long id;
    private final RingBuffer<RequestEvent> requests;
    private final RingBuffer<ReplyEvent> replies;

    public volatile long q1, q2, q3, q4, q5, q6, q7 = 7L;
    public volatile long p1, p2, p3, p4, p5, p6, p7 = 7L;
    private long counter;
    public volatile long r1, r2, r3, r4, r5, r6, r7 = 7L;
    public volatile long s1, s2, s3, s4, s5, s6, s7 = 7L;

    private final SequenceBarrier barrier;
    private final Sequence sequence = new Sequence();
    private final int concurrency;
    private final int batchSize;

    Client(final long id, final RingBuffer<RequestEvent> requests,
           final RingBuffer<ReplyEvent> replies, final int concurrency, final int batchSize) {
      this.id = id;
      this.requests = requests;
      this.replies = replies;
      this.concurrency = concurrency;
      this.batchSize = batchSize;
      this.barrier = replies.newBarrier();
      this.replies.addGatingSequences(sequence);
    }

    @Override
    protected void startUp() throws Exception {
      for (int i = 0; i < concurrency / batchSize; i++) {
        final long hi = requests.next(batchSize);
        final long lo = hi - (batchSize - 1);
        for (long seq = lo; seq <= batchSize; seq++) {
          send(seq);
        }
        requests.publish(lo, hi);
      }
    }

    @Override
    protected void run() {
      try {
        while (true) {
          process();
        }
      } catch (InterruptedException | AlertException e) {
        e.printStackTrace();
        return;
      } catch (TimeoutException ignore) {
      }
    }

    private void process() throws InterruptedException, TimeoutException, AlertException {
      final long last = sequence.get();
      final long lo = last + 1;
      final long hi = barrier.waitFor(lo);
      final int count = (int) (hi - last);
      final long requestHi = requests.next(count);
      final long requestLo = requestHi - count + 1;
      int n = 0;
      for (long i = 0; i < count; i++, n++) {
        final long seq = lo + i;
        final long requestSeq = requestLo + i;
        replies.get(seq);
        send(requestSeq);
        if (n == batchSize) {
          n = 0;
          requests.publish(requestSeq);
          sequence.set(seq);
        }
      }
      requests.publish(requestHi);
      sequence.set(hi);
      counter += count;
    }

    private void send(final long seq) {
      final RequestEvent event = requests.get(seq);
      event.clientId = id;
      event.id = seq;
    }
  }

  private static final int BUFFER_SIZE = 1024 * 64;

  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  public static void main(final String... args) {

    final int concurrency;
    if (args.length > 0) {
      concurrency = Integer.parseInt(args[0]);
    } else {
      concurrency = 10000;
    }

    final int batchSize;
    if (args.length > 0) {
      batchSize = Integer.parseInt(args[1]);
    } else {
      batchSize = 10;
    }

    out.printf("concurrency: %s%n", concurrency);
    out.printf("batch size: %s%n", batchSize);

//    final Reactor reactor = new Reactor();

    final RingBuffer<RequestEvent> requests =
        createSingleProducer(RequestEvent.FACTORY, BUFFER_SIZE, new LiteBlockingWaitStrategy());
    final RingBuffer<ReplyEvent> replies =
        createSingleProducer(ReplyEvent.FACTORY, BUFFER_SIZE, new LiteBlockingWaitStrategy());

    // Client
    final long clientId = 17;
//    final RingBuffer<RequestEvent> clientRequests = reactor.clientRequestQueue();
//    final RingBuffer<ReplyEvent> clientReplies = reactor.clientReplyQueue();
    final Client client = new Client(clientId, requests, replies, concurrency, batchSize);

    // Server
//    final RingBuffer<RequestEvent> serverRequests = reactor.serverRequestQueue();
//    final RingBuffer<ReplyEvent> serverReplies = reactor.serverReplyQueue();
    final Server server = new Server(requests, replies, batchSize);

    // Start
//    reactor.startAsync();
    client.startAsync();
    server.startAsync();

    final ProgressMeter meter = new ProgressMeter(new Supplier<ProgressMeter.Counters>() {
      @Override
      public ProgressMeter.Counters get() {
        return new ProgressMeter.Counters(client.counter, 0);
      }
    });
  }

  private static class Reactor extends AbstractService {

    public static final WaitStrategy WAIT_STRATEGY = new LiteBlockingWaitStrategy();

    private final RequestDispatcher requestDispatcher;
    private final ReplyDispatcher replyDispatcher;

    private Reactor() {
      requestDispatcher = new RequestDispatcher();
      replyDispatcher = new ReplyDispatcher();
    }

    @Override
    protected void doStart() {
      requestDispatcher.startAsync();
      replyDispatcher.startAsync();
    }

    @Override
    protected void doStop() {
      requestDispatcher.stopAsync();
      replyDispatcher.stopAsync();
    }

    static class RequestDispatcher extends AbstractExecutionThreadService {

      private final Sequence sequence = new Sequence();
      private final SequenceBarrier barrier;

      private final RingBuffer<RequestEvent> out =
          createSingleProducer(RequestEvent.FACTORY, BUFFER_SIZE, WAIT_STRATEGY);
      private final RingBuffer<RequestEvent> in =
          createSingleProducer(RequestEvent.FACTORY, BUFFER_SIZE, WAIT_STRATEGY);

      RequestDispatcher() {
        in.addGatingSequences(sequence);
        this.barrier = in.newBarrier();
      }

      @SuppressWarnings({"ForLoopReplaceableByForEach", "InfiniteLoopStatement"})
      @Override
      protected void run() throws Exception {
        while (true) {
          process();
        }
      }

      public RingBuffer<RequestEvent> inQueue() {
        return in;
      }

      public RingBuffer<RequestEvent> outQueue() {
        return out;
      }

      void process() throws InterruptedException, TimeoutException, AlertException {

        final long last = sequence.get();
        final long lo = last + 1;
        final long hi = barrier.waitFor(lo);
        final int count = (int) (hi - last);
        if (count == 0) {
          return;
        }
        handle(lo, hi, count);
        sequence.set(hi);
      }

      private void handle(final long lo, final long hi, final int count) {
        final long outHi = out.next(count);
        final long outLog = outHi - count + 1;
        long outSeq = outLog;
        for (long seq = lo; seq <= hi; seq++, outSeq++) {
          final RequestEvent inEvent = in.get(seq);
          final RequestEvent outEvent = out.get(outSeq);
          outEvent.clientId = inEvent.clientId;
          outEvent.id = inEvent.id;
        }
        out.publish(outHi);
      }
    }

    static class ReplyDispatcher extends AbstractExecutionThreadService {

      private final Sequence sequence = new Sequence();
      private final SequenceBarrier barrier;

      private final RingBuffer<ReplyEvent> out =
          createSingleProducer(ReplyEvent.FACTORY, BUFFER_SIZE, WAIT_STRATEGY);
      private final RingBuffer<ReplyEvent> in =
          createSingleProducer(ReplyEvent.FACTORY, BUFFER_SIZE, WAIT_STRATEGY);

      ReplyDispatcher() {
        in.addGatingSequences(sequence);
        this.barrier = in.newBarrier();
      }

      @SuppressWarnings({"ForLoopReplaceableByForEach", "InfiniteLoopStatement"})
      @Override
      protected void run() throws Exception {
        while (true) {
          process();
        }
      }

      public RingBuffer<ReplyEvent> outQueue() {
        return out;
      }

      public RingBuffer<ReplyEvent> inQueue() {
        return in;
      }

      void process() throws InterruptedException, TimeoutException, AlertException {

        final long last = sequence.get();
        final long lo = last + 1;
        final long hi = barrier.waitFor(lo);
        final int count = (int) (hi - last);
        if (count == 0) {
          return;
        }
        handle(lo, hi, count);
        sequence.set(hi);
      }

      private void handle(final long lo, final long hi, final int count) {
        final long outHi = out.next(count);
        final long outLo = outHi - count + 1;
        long outSeq = outLo;
        for (long seq = lo; seq <= hi; seq++, outSeq++) {
          final ReplyEvent event = in.get(seq);
          final ReplyEvent clientReplyEvent = out.get(outSeq);
          clientReplyEvent.clientId = event.clientId;
          clientReplyEvent.id = event.id;
        }
        out.publish(outHi);
      }
    }

    public RingBuffer<RequestEvent> clientRequestQueue() {
      return requestDispatcher.inQueue();
    }

    public RingBuffer<RequestEvent> serverRequestQueue() {
      return requestDispatcher.outQueue();
    }

    public RingBuffer<ReplyEvent> serverReplyQueue() {
      return replyDispatcher.inQueue();
    }

    public RingBuffer<ReplyEvent> clientReplyQueue() {
      return replyDispatcher.outQueue();
    }
  }
}
