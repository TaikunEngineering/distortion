package engineering.taikun.distortion.serialization.util;

import engineering.taikun.distortion.serialization.api.ByteArray;

import java.nio.ByteBuffer;

/**
 * A
 */
public class ByteBufferWrapper extends ByteArray<ByteBufferWrapper> {

	public final ByteBuffer buffer;

	public ByteBufferWrapper(final ByteBuffer buffer) {
		this.buffer = buffer;
	}

	@Override
	public byte read(final int index) {
		return this.buffer.get(index);
	}

	@Override
	protected void internalWrite(final int index, final byte value) {
		this.buffer.put(index, value);
	}

	@Override
	public int length() {
		return this.buffer.capacity();
	}

	@Override
	protected void internalResize(final int newLength) {
		throw new UnsupportedOperationException("Can't resize ByteBufferWrappers");
	}

	@Override
	public ByteBufferWrapper copy() {
		return null;
	}

	@Override
	public ByteArray slice(final int start, final int end) {
		return new ByteArraySlice<>(null, this, start, end);
	}
}
