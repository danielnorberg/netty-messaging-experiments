import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.WaitStrategy;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class LiteBlockingWaitStrategy implements WaitStrategy
{
  private final Lock lock = new ReentrantLock();
  private final Condition processorNotifyCondition = lock.newCondition();
  private final AtomicBoolean signalNeeded = new AtomicBoolean(false);

  @Override
  public long waitFor(long sequence, Sequence cursorSequence, Sequence dependentSequence, SequenceBarrier barrier)
      throws AlertException, InterruptedException
  {
    long availableSequence;
    if ((availableSequence = cursorSequence.get()) < sequence)
    {
      lock.lock();

      try
      {
        do
        {
          signalNeeded.set(true);

          if ((availableSequence = cursorSequence.get()) >= sequence)
          {
            break;
          }

          barrier.checkAlert();
          processorNotifyCondition.await();
        }
        while ((availableSequence = cursorSequence.get()) < sequence);
      }
      finally
      {
        lock.unlock();
      }
    }

    while ((availableSequence = dependentSequence.get()) < sequence)
    {
      barrier.checkAlert();
    }

    return availableSequence;
  }

  @Override
  public void signalAllWhenBlocking()
  {
    if (signalNeeded.getAndSet(false))
    {
      lock.lock();
      try
      {
        processorNotifyCondition.signalAll();
      }
      finally
      {
        lock.unlock();
      }
    }
  }
}