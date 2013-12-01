import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ReadOnlyBufferException;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

import io.netty.buffer.AbstractByteBuf;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufProcessor;
import io.netty.buffer.DuplicatedByteBuf;
import io.netty.buffer.Unpooled;

/**
 * A derived buffer which forbids any write requests to its parent and also prevents increasing or
 * decreasing the reference count.
 */
public class UnreleasableReadOnlyByteBuf extends AbstractByteBuf {

  private final ByteBuf buffer;

  public UnreleasableReadOnlyByteBuf(ByteBuf buffer) {
    super(buffer.maxCapacity());

    if (buffer instanceof UnreleasableReadOnlyByteBuf || buffer instanceof DuplicatedByteBuf) {
      this.buffer = buffer.unwrap();
    } else {
      this.buffer = buffer;
    }
    setIndex(buffer.readerIndex(), buffer.writerIndex());
  }

  @Override
  public boolean isWritable() {
    return false;
  }

  @Override
  public boolean isWritable(int numBytes) {
    return false;
  }

  @Override
  public ByteBuf unwrap() {
    return buffer;
  }

  @Override
  public ByteBufAllocator alloc() {
    return buffer.alloc();
  }

  @Override
  public ByteOrder order() {
    return buffer.order();
  }

  @Override
  public boolean isDirect() {
    return buffer.isDirect();
  }

  @Override
  public boolean hasArray() {
    return false;
  }

  @Override
  public byte[] array() {
    throw new ReadOnlyBufferException();
  }

  @Override
  public int arrayOffset() {
    throw new ReadOnlyBufferException();
  }

  @Override
  public boolean hasMemoryAddress() {
    return false;
  }

  @Override
  public long memoryAddress() {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ByteBuf discardReadBytes() {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuffer src) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ByteBuf setByte(int index, int value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  protected void _setByte(int index, int value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ByteBuf setShort(int index, int value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  protected void _setShort(int index, int value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ByteBuf setMedium(int index, int value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  protected void _setMedium(int index, int value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ByteBuf setInt(int index, int value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  protected void _setInt(int index, int value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ByteBuf setLong(int index, long value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  protected void _setLong(int index, long value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public int setBytes(int index, InputStream in, int length) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public int setBytes(int index, ScatteringByteChannel in, int length) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public int getBytes(int index, GatheringByteChannel out, int length)
      throws IOException {
    return buffer.getBytes(index, out, length);
  }

  @Override
  public ByteBuf getBytes(int index, OutputStream out, int length)
      throws IOException {
    buffer.getBytes(index, out, length);
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
    buffer.getBytes(index, dst, dstIndex, length);
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
    buffer.getBytes(index, dst, dstIndex, length);
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuffer dst) {
    buffer.getBytes(index, dst);
    return this;
  }

  @Override
  public ByteBuf duplicate() {
    return new UnreleasableReadOnlyByteBuf(this);
  }

  @Override
  public ByteBuf copy(int index, int length) {
    return buffer.copy(index, length);
  }

  @Override
  public ByteBuf slice(int index, int length) {
    return Unpooled.unmodifiableBuffer(buffer.slice(index, length));
  }

  @Override
  public byte getByte(int index) {
    return _getByte(index);
  }

  @Override
  protected byte _getByte(int index) {
    return buffer.getByte(index);
  }

  @Override
  public short getShort(int index) {
    return _getShort(index);
  }

  @Override
  protected short _getShort(int index) {
    return buffer.getShort(index);
  }

  @Override
  public int getUnsignedMedium(int index) {
    return _getUnsignedMedium(index);
  }

  @Override
  protected int _getUnsignedMedium(int index) {
    return buffer.getUnsignedMedium(index);
  }

  @Override
  public int getInt(int index) {
    return _getInt(index);
  }

  @Override
  protected int _getInt(int index) {
    return buffer.getInt(index);
  }

  @Override
  public long getLong(int index) {
    return _getLong(index);
  }

  @Override
  protected long _getLong(int index) {
    return buffer.getLong(index);
  }

  @Override
  public int nioBufferCount() {
    return buffer.nioBufferCount();
  }

  @Override
  public ByteBuffer nioBuffer(int index, int length) {
    return buffer.nioBuffer(index, length).asReadOnlyBuffer();
  }

  @Override
  public ByteBuffer[] nioBuffers(int index, int length) {
    return buffer.nioBuffers(index, length);
  }

  @Override
  public ByteBuffer internalNioBuffer(int index, int length) {
    return nioBuffer(index, length);
  }

  @Override
  public int forEachByte(int index, int length, ByteBufProcessor processor) {
    return buffer.forEachByte(index, length, processor);
  }

  @Override
  public int forEachByteDesc(int index, int length, ByteBufProcessor processor) {
    return buffer.forEachByteDesc(index, length, processor);
  }

  @Override
  public int capacity() {
    return buffer.capacity();
  }

  @Override
  public ByteBuf capacity(int newCapacity) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public final ByteBuf retain() {
    return this;
  }

  @Override
  public final ByteBuf retain(int increment) {
    return this;
  }

  @Override
  public int refCnt() {
    return 1;
  }

  @Override
  public boolean release() {
    return false;
  }

  @Override
  public boolean release(int decrement) {
    return false;
  }


}
