/**
 * Copyright (C) 2013 Spotify AB
 */

import org.jboss.netty.buffer.BigEndianHeapChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

public class EmptyBuffer extends BigEndianHeapChannelBuffer {

  private static final byte[] BUFFER = {};

  public EmptyBuffer() {
    super(BUFFER);
  }

  @Override
  public void clear() {
  }

  @Override
  public void readerIndex(final int readerIndex) {
    if (readerIndex != 0) {
      throw new IndexOutOfBoundsException("Invalid readerIndex: "
                                          + readerIndex + " - Maximum is 0");
    }
  }

  @Override
  public void writerIndex(final int writerIndex) {
    if (writerIndex != 0) {
      throw new IndexOutOfBoundsException("Invalid writerIndex: "
                                          + writerIndex + " - Maximum is 0");
    }
  }

  @Override
  public void setIndex(final int readerIndex, final int writerIndex) {
    if (writerIndex != 0 || readerIndex != 0) {
      throw new IndexOutOfBoundsException("Invalid writerIndex: "
                                          + writerIndex + " - Maximum is " + readerIndex + " or "
                                          + capacity());
    }
  }

  @Override
  public void markReaderIndex() {
    // nop
  }

  @Override
  public void resetReaderIndex() {
    // nop
  }

  @Override
  public void markWriterIndex() {
    // nop
  }

  @Override
  public void resetWriterIndex() {
    // nop
  }

  @Override
  public void discardReadBytes() {
    // nop
  }

  @Override
  public ChannelBuffer readBytes(final int length) {
    checkReadableBytes(length);
    return this;
  }

  @Override
  public ChannelBuffer readSlice(final int length) {
    checkReadableBytes(length);
    return this;
  }

  @Override
  public void readBytes(final byte[] dst, final int dstIndex, final int length) {
    checkReadableBytes(length);
  }

  @Override
  public void readBytes(final byte[] dst) {
    checkReadableBytes(dst.length);
  }

  @Override
  public void readBytes(final ChannelBuffer dst) {
    checkReadableBytes(dst.writableBytes());
  }

  @Override
  public void readBytes(final ChannelBuffer dst, final int length) {
    if (length > dst.writableBytes()) {
      throw new IndexOutOfBoundsException("Too many bytes to be read: Need "
                                          + length + ", maximum is " + dst.writableBytes());
    }
    readBytes(dst, dst.writerIndex(), length);
  }

  @Override
  public void readBytes(final ChannelBuffer dst, final int dstIndex, final int length) {
    checkReadableBytes(length);
  }

  @Override
  public void readBytes(final ByteBuffer dst) {
    checkReadableBytes(dst.remaining());
  }

  @Override
  public int readBytes(final GatheringByteChannel out, final int length) throws IOException {
    checkReadableBytes(length);
    return 0;
  }

  @Override
  public void readBytes(final OutputStream out, final int length) throws IOException {
    checkReadableBytes(length);
  }

  @Override
  public void skipBytes(final int length) {
    checkReadableBytes(length);
  }

  @Override
  public void writeBytes(final byte[] src, final int srcIndex, final int length) {
    checkWritableBytes(length);
  }

  @Override
  public void writeBytes(final ChannelBuffer src, final int length) {
    checkWritableBytes(length);
  }

  @Override
  public void writeBytes(final ChannelBuffer src, final int srcIndex, final int length) {
    checkWritableBytes(length);
  }

  @Override
  public void writeBytes(final ByteBuffer src) {
    checkWritableBytes(src.remaining());
  }

  @Override
  public int writeBytes(final InputStream in, final int length) throws IOException {
    checkWritableBytes(length);
    return 0;
  }

  @Override
  public int writeBytes(final ScatteringByteChannel in, final int length) throws IOException {
    checkWritableBytes(length);
    return 0;
  }

  @Override
  public void writeZero(final int length) {
    checkWritableBytes(length);
  }

  private void checkWritableBytes(final int length) {
    if (length > 0) {
      throw new IndexOutOfBoundsException("Writable bytes exceeded - Need "
                                          + length + ", maximum is " + 0);
    }
  }
}
