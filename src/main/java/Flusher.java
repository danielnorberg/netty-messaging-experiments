import io.netty.channel.Channel;

public class Flusher {

  private final Channel channel;

  private boolean registered;
  private Runnable flusher = new Runnable() {
    @Override
    public void run() {
      registered = false;
      channel.flush();
    }
  };

  public Flusher(final Channel channel) {
    this.channel = channel;
  }

  public void enqueue() {
    enqueue(1);
  }

  public void enqueue(final int messages) {
    if (!registered) {
      registered = true;
      channel.eventLoop().execute(flusher);
    }
  }
}
