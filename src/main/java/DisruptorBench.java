import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractExecutionThreadService;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.Sequencer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.util.PaddedLong;

import org.jboss.netty.buffer.ChannelBuffer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

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
    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

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

      final Request request;
      final long clientId;

      // Receive request
      {
        long seq = sequence.get() + 1;
        barrier.waitFor(seq);
        final RequestEvent requestEvent = requests.get(seq);
        request = requestEvent.getRequest();
        clientId = requestEvent.getClientId();
        sequence.set(seq);
      }

      // Send reply
      {
        final Reply reply = request.makeReply(418);
        final long seq = replies.next();
        final ReplyEvent replyEvent = replies.get(seq);
        replyEvent.setClientId(clientId);
        replyEvent.setReply(reply);
        replies.publish(seq);
      }
    }
  }

  static class Client extends AbstractExecutionThreadService {

    static final int CONCURRENCY = 1;

    private final long id;
    private final RingBuffer<RequestEvent> requests;
    private final RingBuffer<ReplyEvent> replies;
    private volatile long requestIdCounter;

    private final SequenceBarrier barrier;
    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);


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
      for (int i = 0; i < CONCURRENCY; i++) {
        send();
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
      long seq = sequence.get() + 1;
      barrier.waitFor(seq);
      final ReplyEvent replyEvent = replies.get(seq);
      send();
    }

    private void send() {
      // TODO (dano): make it possible to interrupt next()
      final long i = requests.next();
      final RequestEvent event = requests.get(i);
      event.setClientId(id);
      event.setRequest(new Request(requestIdCounter, EMPTY_BUFFER));
      requestIdCounter++;
      requests.publish(i);
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

    private final Semaphore semaphore = new Semaphore(0);
    private final List<AbstractEventHandler> handlers = Lists.newArrayList();
    private final List<RingBuffer<RequestEvent>> serverRequestQueues = Lists.newArrayList();

    private final PaddedLong serverIndex = new PaddedLong();
    private final Map<Long, RingBuffer<ReplyEvent>> clientReplyQueues = Maps.newHashMap();

    @Override
    protected void run() throws Exception {
      while (true) {
        semaphore.acquire();
        semaphore.drainPermits();
        for (AbstractEventHandler handler : handlers) {
          handler.process();
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
                                                                  new YieldingWaitStrategy());
      serverRequestQueues.add(queue);
      return queue;
    }

    public RingBuffer<ReplyEvent> serverReplyQueue() {
      final ReplyEventHandler handler = new ReplyEventHandler();
      handlers.add(handler);
      return handler.getQueue();
    }

    public RingBuffer<ReplyEvent> clientReplyQueue(final long clientId) {
      final RingBuffer<ReplyEvent> queue = createSingleProducer(ReplyEvent.FACTORY, BUFFER_SIZE,
                                                                new YieldingWaitStrategy());
      clientReplyQueues.put(clientId, queue);
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

      void reset() {
        notified.set(false);
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

        reset();

        long seq = sequence.get() + 1;
        if (seq == -1) {
          // TODO: this is unexpected given that the reactor was notified
          return;
        }
        barrier.waitFor(seq);
        final E event = queue.get(seq);
        handle(event);
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
        final RingBuffer<RequestEvent> queue =
            serverRequestQueues.get((int) (i % serverRequestQueues.size()));
        // TODO (dano): use tryNext
        final long seq = queue.next();
        final RequestEvent serverEvent = queue.get(seq);
        serverEvent.setClientId(event.getClientId());
        serverEvent.setRequest(event.getRequest());
        queue.publish(seq);
      }
    }

    private class ReplyEventHandler extends AbstractEventHandler<ReplyEvent> {

      private ReplyEventHandler() {
        super(ReplyEvent.FACTORY);
      }

      @Override
      protected void handle(final ReplyEvent event) {
        final RingBuffer<ReplyEvent> queue = clientReplyQueues.get(event.getClientId());
        // TODO (dano): use tryNext
        final long seq = queue.next();
        final ReplyEvent clientReplyEvent = queue.get(seq);
        clientReplyEvent.setClientId(event.getClientId());
        clientReplyEvent.setReply(event.getReply());
        queue.publish(seq);
      }
    }
  }


}
