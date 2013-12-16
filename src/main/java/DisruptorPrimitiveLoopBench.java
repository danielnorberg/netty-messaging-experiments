import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractExecutionThreadService;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WaitStrategy;

import org.jboss.netty.buffer.ChannelBuffer;

import java.util.List;

import static com.lmax.disruptor.RingBuffer.createSingleProducer;
import static java.lang.Math.max;
import static java.lang.System.out;

public class DisruptorPrimitiveLoopBench {

  static final ChannelBuffer EMPTY_BUFFER = new EmptyBuffer();

  static final int CONCURRENCY = 1000;
  static final int BATCH_SIZE = 10;

  public static final LiteBlockingWaitStrategy WAIT_STRATEGY = new LiteBlockingWaitStrategy();

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

  static class MessageEvent {

    public static final EventFactory<MessageEvent> FACTORY = new Factory();

    long actorId;
    long id;

    static class Factory implements EventFactory<MessageEvent> {

      @Override
      public MessageEvent newInstance() {
        return new MessageEvent();
      }
    }
  }

  static class Actor extends AbstractExecutionThreadService {

    private final long id;
    private final RingBuffer<MessageEvent> in;
    private final RingBuffer<MessageEvent> out;

    public volatile long q1, q2, q3, q4, q5, q6, q7 = 7L;
    public volatile long p1, p2, p3, p4, p5, p6, p7 = 7L;
    private long counter;
    public volatile long r1, r2, r3, r4, r5, r6, r7 = 7L;
    public volatile long s1, s2, s3, s4, s5, s6, s7 = 7L;

    private final SequenceBarrier barrier;
    private final Sequence sequence = new Sequence();
    private final int concurrency;

    Actor(final long id, final RingBuffer<MessageEvent> in,
          final RingBuffer<MessageEvent> out, final int concurrency) {
      this.id = id;
      this.in = in;
      this.out = out;
      this.concurrency = concurrency;
      barrier = in.newBarrier();
      in.addGatingSequences(sequence);
    }

    @Override
    protected void startUp() throws Exception {
      if (concurrency == 0) {
        return;
      }
      final long hi = out.next(concurrency);
      final long lo = hi - (concurrency - 1);
      for (long seq = lo; seq <= hi; seq++) {
        send(id, seq);
      }
      out.publish(lo, hi);
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
      final long requestHi = out.next(count);
      final long requestLo = requestHi - count + 1;
      int n = 0;
      for (long i = 0; i < count; i++, n++) {
        final long seq = lo + i;
        final long requestSeq = requestLo + i;
        final MessageEvent messageEvent = in.get(seq);
        send(messageEvent.actorId, requestSeq);
        if (n == BATCH_SIZE) {
          n = 0;
          out.publish(requestSeq);
          sequence.set(seq);
        }
      }
      out.publish(requestHi);
      sequence.set(hi);
      counter += count;
    }

    private void send(final long actorId, final long seq) {
      final MessageEvent event = out.get(seq);
      event.actorId = actorId;
      event.id = seq;
    }
  }

  private static final int BUFFER_SIZE = 1024 * 64;

  public static void main(final String... args) {

    final int instances;
    if (args.length > 0) {
      instances = Integer.parseInt(args[0]);
    } else {
      instances = max(2, Runtime.getRuntime().availableProcessors() / 2);
    }

    out.printf("instances: %s%n", instances);

    // Actors
    final List<RingBuffer<MessageEvent>> buffers = Lists.newArrayList();
    final List<Actor> actors = Lists.newArrayList();

    for (int i = 0; i < instances; i++) {
      buffers.add(createSingleProducer(MessageEvent.FACTORY, BUFFER_SIZE, WAIT_STRATEGY));
    }

    for (int i = 0; i < instances; i++) {
      final RingBuffer<MessageEvent> in = buffers.get((i + 1) % buffers.size());
      final RingBuffer<MessageEvent> out = buffers.get(i);
      actors.add(new Actor(i, in, out, CONCURRENCY));
    }

    // Start
    for (final Actor actor : actors) {
      actor.startAsync();
    }

    final ProgressMeter meter = new ProgressMeter("loops", new Supplier<ProgressMeter.Counters>() {
      @Override
      public ProgressMeter.Counters get() {
        return new ProgressMeter.Counters(actors.get(0).counter, 0);
      }
    });
  }
}
