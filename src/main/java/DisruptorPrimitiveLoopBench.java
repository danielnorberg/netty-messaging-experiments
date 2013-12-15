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
import java.util.concurrent.Semaphore;

import jsr166.concurrent.atomic.AtomicBoolean;

import static com.lmax.disruptor.RingBuffer.createSingleProducer;
import static java.lang.Math.min;

public class DisruptorPrimitiveLoopBench {

  static final ChannelBuffer EMPTY_BUFFER = new EmptyBuffer();

  static final int CONCURRENCY = 100;
  static final int BATCH_SIZE = 100;
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
    private long counter = 0;
    public volatile long r1, r2, r3, r4, r5, r6, r7 = 7L;
    public volatile long s1, s2, s3, s4, s5, s6, s7 = 7L;

    private final SequenceBarrier barrier;
    private final Sequence sequence = new Sequence();

    Actor(final long id, final RingBuffer<MessageEvent> in,
          final RingBuffer<MessageEvent> out) {
      this.id = id;
      this.in = in;
      this.out = out;
      barrier = in.newBarrier();
      in.addGatingSequences(sequence);
    }

    @Override
    protected void startUp() throws Exception {
      final long hi = out.next(CONCURRENCY);
      final long lo = hi - (CONCURRENCY - 1);
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
      final long requestLo = in.getCursor() + 1;
      final long requestHi = in.next(count);
      for (long i = 0; i < count; i++) {
        final MessageEvent messageEvent = out.get(lo + i);
        send(messageEvent.actorId, requestLo + i);
      }
      sequence.set(hi);
      in.publish(requestHi);
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

    final int n = 4;

    // Actors
    final List<RingBuffer<MessageEvent>> buffers = Lists.newArrayList();
    final List<Actor> actors = Lists.newArrayList();

    for (int i = 0; i < n; i++) {
      buffers.add(createSingleProducer(MessageEvent.FACTORY, BUFFER_SIZE, WAIT_STRATEGY));
    }

    for (int i = 0; i < n; i++) {
      final RingBuffer<MessageEvent> in = buffers.get((i + 1) % buffers.size());
      final RingBuffer<MessageEvent> out = buffers.get(i);
      actors.add(new Actor(i, in, out));
    }

    // Start
    for (final Actor actor : actors) {
      actor.startAsync();
    }

    final ProgressMeter meter = new ProgressMeter(new Supplier<ProgressMeter.Counters>() {
      @Override
      public ProgressMeter.Counters get() {
        long sum = 0;
        for (Actor actor : actors) {
          sum += actor.counter;
        }
        return new ProgressMeter.Counters(sum, 0);
      }
    });
  }
}
