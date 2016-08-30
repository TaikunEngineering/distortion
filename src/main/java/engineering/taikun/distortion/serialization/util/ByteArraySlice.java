package engineering.taikun.distortion.serialization.util;

import engineering.taikun.distortion.serialization.api.ByteArray;
import engineering.taikun.distortion.serialization.api.ByteArrayFactory;
import org.jetbrains.annotations.Nullable;

/**
 * A generic implementation of a ByteArray-slice with a backing {@link ByteArray}
 *
 * @param <BA> The generic type of the backing {@link ByteArray} and the {@link ByteArrayFactory}
 */
public class ByteArraySlice<BA extends ByteArray<BA>> extends ByteArray<BA> {

	public final @Nullable ByteArrayFactory<BA> factory;
	public final ByteArray source;
	public final int start;
	public final int end;

	public ByteArraySlice(
			final @Nullable ByteArrayFactory<BA> factory, final ByteArray source, final int start, final int end
	) {
		this.factory = factory;
		this.source = source;
		this.start = start;
		this.end = end;

		if (start < 0 || start > source.length() || end < start || end > source.length()) {
			throw new IndexOutOfBoundsException("start: " + start + ", end: " + end + ", source-length: " + source.length());
		}
	}

	@Override
	public byte read(final int index) {
		if (index < 0 || index >= length()) {
			throw new IndexOutOfBoundsException("index: " + index + ", array-length: " + length());
		}

		return this.source.read(this.start + index);
	}

	@Override
	public void internalWrite(final int index, final byte value) {
		if (index < 0 || index >= length()) {
			throw new IndexOutOfBoundsException("index: " + index + ", array-length: " + length());
		}

		this.source.write(this.start + index, value);
	}

	@Override
	public int length() {
		return this.end - this.start;
	}

	@Override
	public void internalResize(final int newLength) {
		throw new UnsupportedOperationException("Can't resize array slices");
	}

	@Override
	public BA copy() {
		if (this.factory == null)
			return null;

		final BA temp = this.factory.allocate(length());

		for (int i = 0; i < length(); i++) {
			temp.write(i, this.source.read(this.start + i));
		}

		return temp;
	}

	@Override
	public ByteArray slice(final int start, final int end) {
		return new ByteArraySlice<>(this.factory, this.source, this.start + start, this.start + end);
	}
}
