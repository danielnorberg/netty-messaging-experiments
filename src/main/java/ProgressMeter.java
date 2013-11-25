import com.google.common.base.Supplier;

import java.util.ArrayDeque;

public class ProgressMeter {

  private final Supplier<Counters> counters;

  public static class Counters {

    final long requests;
    final long latency;

    public Counters(final long requests, final long latency) {
      this.requests = requests;
      this.latency = latency;
    }
  }


  static class Delta {

    Delta(final long ops, final long time) {
      this.ops = ops;
      this.time = time;
    }

    public final long ops;
    public final long time;
  }

  private long lastRows = 0;
  private long lastTime = System.nanoTime();
  private long lastLatency = 0;
  private final long interval = 1000;

  final private String unit;

  final private ArrayDeque<Delta> deltas = new ArrayDeque<Delta>();

  private volatile boolean run = true;

  private final Thread worker;

  public ProgressMeter(final Supplier<Counters> countersSupplier) {
    this("ops", countersSupplier);
  }

  public ProgressMeter(final String unit, final Supplier<Counters> countersSupplier) {
    this.unit = unit;
    this.counters = countersSupplier;
    worker = new Thread(new Runnable() {
      public void run() {
        while (run) {
          try {
            Thread.sleep(interval);
          } catch (InterruptedException e) {
            continue;
          }
          progress();
        }
      }
    });
    worker.start();
  }

  private void progress() {
    final Counters counters = this.counters.get();

    final long count = counters.requests;
    final long latency = counters.latency;

    final long time = System.nanoTime();

    final long delta = count - lastRows;
    final long deltaTime = time - lastTime;
    final long deltaLatency = latency - lastLatency;

    deltas.add(new Delta(delta, deltaTime));

    if (deltas.size() > 10) {
      deltas.pop();
    }

    long opSum = 0;
    long timeSum = 0;

    for (final Delta d : deltas) {
      timeSum += d.time;
      opSum += d.ops;
    }

    final long operations = deltaTime == 0 ? 0 : 1000000000 * delta / deltaTime;
    final long averagedOperations = timeSum == 0 ? 0 : 1000000000 * opSum / timeSum;
    final double averageLatency = opSum == 0 ? 0 : deltaLatency / (1000000.d * opSum);

    System.out.printf("%,10d (%,10d) %s/s. %,10.9f ms average latency. %,10d %s total.\n",
                      operations, averagedOperations, unit, averageLatency, count, unit);
    System.out.flush();

    lastRows = count;
    lastTime = time;
    lastLatency = latency;
  }

  public void finish() {
    run = false;
    worker.interrupt();
    try {
      worker.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    progress();
  }

//  public void inc(final long ops, final long latency) {
//    this.operations.add(ops);
//    this.latency.add(latency);
//  }
}
