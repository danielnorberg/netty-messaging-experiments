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

import java.util.concurrent.Semaphore;

import jsr166.concurrent.atomic.AtomicBoolean;

import static com.lmax.disruptor.RingBuffer.createSingleProducer;
import static java.lang.Math.min;

public class DisruptorPrimitiveSingleServerClientSplitReactorReqRepBench {

  private static class ReplyEvent {

    public static final EventFactory<ReplyEvent> FACTORY = new Factory();

    long clientId;
    long id;

    // Used by the reactor
    EventQueue<ReplyEvent> queue;

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
    private final SequenceBarrier barrier;
    private final Sequence sequence = new Sequence();
    private final int batchSize;

    Server(final RingBuffer<RequestEvent> requests,
           final RingBuffer<ReplyEvent> replies, final int batchSize) {
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
      int remaining = count;
      long seq = lo;

      while (remaining > 0) {
        final int batch = Math.min(batchSize, remaining);
        final long batchLo = seq;
        final long batchHi = seq + batch - 1;
        process(batchLo, batchHi, batch);
        seq += batch;
        remaining -= batch;
      }
    }

    private void process(final long lo, final long hi, final int count) {
      final long replyLast = replies.getCursor();
      final long replyLo = replyLast + 1;
      final long replyHi = replies.next(count);

      for (int i = 0; i < count; i++) {
        final RequestEvent requestEvent = requests.get(lo + i);
        final long clientId = requestEvent.clientId;
        final ReplyEvent replyEvent = replies.get(replyLo + i);
        replyEvent.clientId = clientId;
        replyEvent.id = requestEvent.id;
      }

      sequence.set(hi);
      replies.publish(replyLo, replyHi);
    }
  }

  static class Client extends AbstractExecutionThreadService {

    private final long id;
    private final RingBuffer<RequestEvent> requests;
    private final RingBuffer<ReplyEvent> replies;

    public volatile long q1, q2, q3, q4, q5, q6, q7 = 7L;
    public volatile long p1, p2, p3, p4, p5, p6, p7 = 7L;
    private long requestIdCounter = 0;
    public volatile long r1, r2, r3, r4, r5, r6, r7 = 7L;
    public volatile long s1, s2, s3, s4, s5, s6, s7 = 7L;

    private final SequenceBarrier barrier;
    private final Sequence sequence = new Sequence();
    private final int concurrency;

    Client(final long id,
           final RingBuffer<RequestEvent> requests,
           final RingBuffer<ReplyEvent> replies, final int concurrency) {
      this.id = id;
      this.requests = requests;
      this.replies = replies;
      this.concurrency = concurrency;
      this.barrier = replies.newBarrier();
      this.replies.addGatingSequences(sequence);
    }

    @Override
    protected void startUp() throws Exception {
      sendBatch();
    }

    private void sendBatch() {
      final long hi = requests.next(concurrency);
      final long lo = hi - (concurrency - 1);
      for (long seq = lo; seq <= hi; seq++) {
        send(seq);
      }
      requests.publish(lo, hi);
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
      final long requestLo = requests.getCursor() + 1;
      final long requestHi = requests.next(count);
      for (long i = 0; i < count; i++) {
        replies.get(lo + i);
        send(requestLo + i);
      }
      sequence.set(hi);
      requests.publish(requestHi);
    }

    private void send(final long seq) {
      // TODO (dano): make it possible to interrupt sequence()
      final RequestEvent event = requests.get(seq);
      event.clientId = id;
      event.id = requestIdCounter++;
    }
  }

  private static final int BUFFER_SIZE = 1024 * 64;

  public static void main(final String... args) {

    final int batchSize;
    if (args.length > 0) {
      batchSize = Integer.parseInt(args[0]);
    } else {
      batchSize = 10;
    }

    final int concurrency;
    if (args.length > 1) {
      concurrency = Integer.parseInt(args[1]);
    } else {
      concurrency = 10000;
    }

    System.out.printf("batch size: %s%n", batchSize);
    System.out.printf("concurrency: %s%n", concurrency);

    final Reactor reactor = new Reactor(batchSize);

    // Client
    final long clientId = 17;
    final Client client = new Client(clientId,
                                     reactor.clientRequestQueue(),
                                     reactor.clientReplyQueue(), concurrency);

    // Server
    final Server server = new Server(reactor.serverRequestQueue(),
                                     reactor.serverReplyQueue(), batchSize);

    // Start
    reactor.startAsync();
    client.startAsync();
    server.startAsync();

    final ProgressMeter meter = new ProgressMeter(new Supplier<ProgressMeter.Counters>() {
      @Override
      public ProgressMeter.Counters get() {
        return new ProgressMeter.Counters(client.requestIdCounter, 0);
      }
    });
  }

  static class EventQueue<E> {

    private final RingBuffer<E> buffer;
    private final long batchSize;

    EventQueue(final RingBuffer<E> buffer, final long batchSize) {
      this.buffer = buffer;
      this.batchSize = batchSize;
    }
  }

  private static class Reactor extends AbstractService {

    public static final WaitStrategy WAIT_STRATEGY = new LiteBlockingWaitStrategy();
    private final RequestDispatcher requestDispatcher;
    private final ReplyDispatcher replyDispatcher;

    private Reactor(final int batchSize) {
      requestDispatcher = new RequestDispatcher(batchSize);
      replyDispatcher = new ReplyDispatcher(batchSize);
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

      private final Semaphore semaphore = new Semaphore(0);
      private final RequestEventHandler handler;
      private final RingBuffer<RequestEvent> out =
          createSingleProducer(RequestEvent.FACTORY, BUFFER_SIZE, WAIT_STRATEGY);

      RequestDispatcher(final int batchSize) {
        this.handler = new RequestEventHandler(batchSize);
      }

      @SuppressWarnings({"ForLoopReplaceableByForEach", "InfiniteLoopStatement"})
      @Override
      protected void run() throws Exception {
        while (true) {
          handler.process();
        }
      }

      public RingBuffer<RequestEvent> inQueue() {
        return handler.getQueue();
      }

      public RingBuffer<RequestEvent> outQueue() {
        return out;
      }

      private class RequestEventHandler extends AbstractEventHandler<RequestEvent> {

        private RequestEventHandler(final int batchSize) {
          super(RequestEvent.FACTORY, semaphore, batchSize);
        }

        @Override
        protected void handle(final RingBuffer<RequestEvent> in, final long lo, final long hi) {
          final int n = (int) (hi - lo + 1);
          final long outHi = out.next(n);
          long outSeq = outHi - n + 1;
          for (long seq = lo; seq <= hi; seq++, outSeq++) {
            final RequestEvent inEvent = in.get(seq);
            final RequestEvent outEvent = out.get(outSeq);
            outEvent.clientId = inEvent.clientId;
            outEvent.id = inEvent.id;
          }
          out.publish(outHi);
        }
      }
    }

    static class ReplyDispatcher extends AbstractExecutionThreadService {

      private final Semaphore semaphore = new Semaphore(0);
      private final ReplyEventHandler handler;
      private final RingBuffer<ReplyEvent> out =
          createSingleProducer(ReplyEvent.FACTORY, BUFFER_SIZE, WAIT_STRATEGY);

      ReplyDispatcher(final int batchSize) {
        this.handler = new ReplyEventHandler(batchSize);
      }

      @SuppressWarnings({"ForLoopReplaceableByForEach", "InfiniteLoopStatement"})
      @Override
      protected void run() throws Exception {
        while (true) {
          handler.process();
        }
      }

      public RingBuffer<ReplyEvent> outQueue() {
        return out;
      }

      public RingBuffer<ReplyEvent> inQueue() {
        return handler.getQueue();
      }


      private class ReplyEventHandler extends AbstractEventHandler<ReplyEvent> {

        private ReplyEventHandler(final int batchSize) {
          super(ReplyEvent.FACTORY, semaphore, batchSize);
        }

        @Override
        protected void handle(final RingBuffer<ReplyEvent> in, final long lo,
                              final long hi) {
          for (long i = lo; i <= hi; i++) {
            final ReplyEvent event = in.get(i);
            final long seq = out.next();
            final ReplyEvent clientReplyEvent = out.get(seq);
            clientReplyEvent.clientId = event.clientId;
            clientReplyEvent.id = event.id;
            out.publish(seq);
          }
        }
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


    private static abstract class AbstractEventHandler<E> implements WaitStrategy {

      private final RingBuffer<E> queue;
      private final Sequence sequence = new Sequence();
      private final AtomicBoolean notified = new AtomicBoolean();
      private final Semaphore semaphore;
      private final int batchSize;

      protected AbstractEventHandler(final EventFactory<E> eventFactory,
                                     final Semaphore semaphore, final int batchSize) {
        this.semaphore = semaphore;
        this.batchSize = batchSize;
        this.queue = createSingleProducer(eventFactory, BUFFER_SIZE, this);
        queue.addGatingSequences(sequence);
      }

      public RingBuffer<E> getQueue() {
        return queue;
      }

      @Override
      public long waitFor(final long sequence, final Sequence cursor,
                          final Sequence dependentSequence,
                          final SequenceBarrier barrier)
          throws AlertException, InterruptedException, TimeoutException {
        throw new UnsupportedOperationException();
      }

      @Override
      public void signalAllWhenBlocking() {
        if (notified.compareAndSet(false, true)) {
          semaphore.release();
        }
      }

      void process() throws InterruptedException, TimeoutException, AlertException {
        if (!notified.get()) {
          return;
        }

        notified.set(false);

        final long last = sequence.get();
        final long lo = last + 1;
        final long hi = queue.getCursor();
        final int count = (int) (hi - last);
        if (count == 0) {
          return;
        }

        int remaining = count;
        long seq = lo;
        while (remaining > 0) {
          final int n = min(remaining, batchSize);
          handle(queue, seq, seq + n - 1);
          remaining -= n;
          seq += n;
        }

        sequence.set(hi);
      }

      protected abstract void handle(final RingBuffer<E> queue, final long lo, final long hi);
    }


  }
}
