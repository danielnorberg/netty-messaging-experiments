import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractExecutionThreadService;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.PhasedBackoffWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.util.PaddedLong;

import org.jboss.netty.buffer.ChannelBuffer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import jsr166.concurrent.atomic.AtomicBoolean;

import static com.lmax.disruptor.RingBuffer.createSingleProducer;

public class DisruptorBench {

  final static ChannelBuffer EMPTY_BUFFER = new EmptyBuffer();

  static class ReactorWaitStrategy implements WaitStrategy {

    @Override
    public long waitFor(final long l, final Sequence sequence, final Sequence sequence2,
                        final SequenceBarrier sequenceBarrier)
        throws AlertException, InterruptedException, TimeoutException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void signalAllWhenBlocking() {
      throw new UnsupportedOperationException();
    }
  }

  private static class ReplyEvent {

    public static final EventFactory<ReplyEvent> FACTORY = new Factory();

    long clientId;
    Reply reply;

    public long getClientId() {
      return clientId;
    }

    public void setClientId(final long clientId) {
      this.clientId = clientId;
    }

    public Reply getReply() {
      return reply;
    }

    public void setReply(final Reply reply) {
      this.reply = reply;
    }

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
    Request request;

    public long getClientId() {
      return clientId;
    }

    public void setClientId(final long clientId) {
      this.clientId = clientId;
    }

    public Request getRequest() {
      return request;
    }

    public void setRequest(final Request request) {
      this.request = request;
    }

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

    Server(final RingBuffer<RequestEvent> requests,
           final RingBuffer<ReplyEvent> replies) {
      this.requests = requests;
      this.replies = replies;
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
      final long next = last + 1;
      final long seq = barrier.waitFor(next);

      final int batch = (int) (seq - last);

      final long replyLast = replies.getCursor();
      final long replyNext = replyLast + 1;
      final long replySeq = replies.next(batch);

      for (long i = 0; i < batch; i++) {
        final RequestEvent requestEvent = requests.get(next + i);
        final Request request = requestEvent.getRequest();
        final long clientId = requestEvent.getClientId();
        final Reply reply = request.makeReply(418);
        final ReplyEvent replyEvent = replies.get(replyNext + i);
        replyEvent.setClientId(clientId);
        replyEvent.setReply(reply);
      }

      sequence.set(seq);
      replies.publish(replySeq);
    }
  }

  static class Client extends AbstractExecutionThreadService {

    static final int CONCURRENCY = 10000;

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

    Client(final long id,
           final RingBuffer<RequestEvent> requests,
           final RingBuffer<ReplyEvent> replies) {
      this.id = id;
      this.requests = requests;
      this.replies = replies;
      this.barrier = replies.newBarrier();
      this.replies.addGatingSequences(sequence);
    }

    @Override
    protected void startUp() throws Exception {
      final long hi = requests.next(CONCURRENCY);
      final long lo = hi - (CONCURRENCY - 1);
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
      final long next = last + 1;
      final long replySeq = barrier.waitFor(next);
      final int batch = (int) (replySeq - last);
      final long requestNext = requests.getCursor() + 1;
      final long requestSeq = requests.next(batch);
      for (long i = 0; i < batch; i++) {
        final ReplyEvent replyEvent = replies.get(next + i);
        send(requestNext + i);
      }
      sequence.set(replySeq);
      requests.publish(requestSeq);
    }

    private void send(final long seq) {
      // TODO (dano): make it possible to interrupt sequence()
      final RequestEvent event = requests.get(seq);
      event.setClientId(id);
      event.setRequest(new Request(requestIdCounter++, EMPTY_BUFFER));
    }
  }

  private static final int BUFFER_SIZE = 1024 * 64;

  public static void main(final String... args) {

    final Reactor reactor = new Reactor();

    // Client
    final long clientId = 17;
    final Client client = new Client(clientId,
                                     reactor.clientRequestQueue(),
                                     reactor.clientReplyQueue(clientId));

    // Server
    final Server server = new Server(reactor.serverRequestQueue(),
                                     reactor.serverReplyQueue());

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

  private static class Reactor extends AbstractExecutionThreadService {

    static class EventQueue<E> {

      final RingBuffer<E> buffer;

      public volatile long q1, q2, q3, q4, q5, q6, q7 = 7L;
      public volatile long p1, p2, p3, p4, p5, p6, p7 = 7L;
      private long prev = -1;
      private long sequence = -1;
      public volatile long r1, r2, r3, r4, r5, r6, r7 = 7L;
      public volatile long s1, s2, s3, s4, s5, s6, s7 = 7L;

      EventQueue(final RingBuffer<E> buffer) {
        this.buffer = buffer;
      }

      public void publish() {
        if (sequence == prev) {
          return;
        }

        buffer.publish(sequence);
        prev = sequence;
      }
    }

    public static final WaitStrategy WAIT_STRATEGY = new YieldingWaitStrategy();
//        PhasedBackoffWaitStrategy.withLock(1, 10, TimeUnit.MILLISECONDS);
    private final Semaphore semaphore = new Semaphore(0);
    private final List<AbstractEventHandler> handlers = Lists.newArrayList();
    private final List<EventQueue<RequestEvent>> serverRequestQueues = Lists.newArrayList();

    private final PaddedLong serverIndex = new PaddedLong();
//    private final Map<Long, EventQueue<ReplyEvent>> clientReplyQueues = Maps.newHashMap();
    // TODO (dano): use a LongToObject map
    private final List<EventQueue<ReplyEvent>> clientReplyQueues = Lists.newArrayList();

    @Override
    protected void run() throws Exception {
      while (true) {
        semaphore.acquire();
        semaphore.drainPermits();
        for (AbstractEventHandler handler : handlers) {
          handler.process();
        }
        for (EventQueue<?> queue : clientReplyQueues) {
          if (queue != null) {
            queue.publish();
          }
        }
        for (EventQueue<?> queue : serverRequestQueues) {
          queue.publish();
        }
      }
    }

    public RingBuffer<RequestEvent> clientRequestQueue() {
      final RequestEventHandler handler = new RequestEventHandler();
      handlers.add(handler);
      return handler.getQueue();
    }

    public RingBuffer<RequestEvent> serverRequestQueue() {
      final RingBuffer<RequestEvent> queue = createSingleProducer(RequestEvent.FACTORY, BUFFER_SIZE,
                                                                  WAIT_STRATEGY);
      serverRequestQueues.add(new EventQueue<>(queue));
      return queue;
    }

    public RingBuffer<ReplyEvent> serverReplyQueue() {
      final ReplyEventHandler handler = new ReplyEventHandler();
      handlers.add(handler);
      return handler.getQueue();
    }

    public RingBuffer<ReplyEvent> clientReplyQueue(final long clientId) {
      final RingBuffer<ReplyEvent> queue = createSingleProducer(ReplyEvent.FACTORY, BUFFER_SIZE,
                                                                WAIT_STRATEGY);
      while (clientReplyQueues.size() <= clientId) {
        clientReplyQueues.add(null);
      }
      clientReplyQueues.set((int) clientId, new EventQueue<>(queue));
      return queue;
    }


    private abstract class AbstractEventHandler<E> implements WaitStrategy {

      private final RingBuffer<E> queue;
      private final Sequence sequence = new Sequence();
      private final SequenceBarrier barrier;
      private final AtomicBoolean notified = new AtomicBoolean();

      protected AbstractEventHandler(final EventFactory<E> eventFactory) {
        this.queue = createSingleProducer(eventFactory, BUFFER_SIZE, this);
        this.barrier = queue.newBarrier();
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
        long availableSequence = dependentSequence.get();
        return (availableSequence < sequence) ? -1 : availableSequence;
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

        long next = sequence.get() + 1;
        long seq = barrier.waitFor(next);
        if (seq == -1) {
          return;
        }
        for (long i = next; i <= seq; i++) {
          final E event = queue.get(i);
          handle(event);
        }
        sequence.set(seq);
      }

      protected abstract void handle(final E event);
    }

    private class RequestEventHandler extends AbstractEventHandler<RequestEvent> {

      private RequestEventHandler() {
        super(RequestEvent.FACTORY);
      }

      @Override
      protected void handle(final RequestEvent event) {
        final long i = serverIndex.get();
        serverIndex.set(i + 1);
        final int index = (int) (i % serverRequestQueues.size());
        final EventQueue<RequestEvent> queue = serverRequestQueues.get(index);
        // TODO (dano): use tryNext
        // TODO (dano): batching
        final long seq = queue.buffer.next();
        queue.sequence = seq;
        final RequestEvent serverEvent = queue.buffer.get(seq);
        serverEvent.setClientId(event.getClientId());
        serverEvent.setRequest(event.getRequest());
      }
    }

    private class ReplyEventHandler extends AbstractEventHandler<ReplyEvent> {

      private ReplyEventHandler() {
        super(ReplyEvent.FACTORY);
      }

      @Override
      protected void handle(final ReplyEvent event) {
        final EventQueue<ReplyEvent> queue = clientReplyQueues.get((int) event.getClientId());
        // TODO (dano): use tryNext
        // TODO (dano): batching
        final long seq = queue.buffer.next();
        queue.sequence = seq;
        final ReplyEvent clientReplyEvent = queue.buffer.get(seq);
        clientReplyEvent.setClientId(event.getClientId());
        clientReplyEvent.setReply(event.getReply());
      }
    }
  }


}
