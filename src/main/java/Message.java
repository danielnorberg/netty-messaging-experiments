import org.jboss.netty.buffer.ChannelBuffer;

/**
 * Copyright (C) 2013 Spotify AB
 */

public interface Message {

  void serialize(final ChannelBuffer buffer);

  int serializedSize();
}
