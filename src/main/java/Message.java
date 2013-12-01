import org.jboss.netty.buffer.ChannelBuffer;

public interface Message {

  void serialize(final ChannelBuffer buffer);

  int serializedSize();
}
