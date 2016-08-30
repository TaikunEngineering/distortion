package engineering.taikun.distortion.serialization.util;

import engineering.taikun.distortion.serialization.api.ByteArray;
import engineering.taikun.distortion.serialization.api.ByteArrayFactory;
import org.jetbrains.annotations.Nullable;

/**
 * <p>Make a {@link ByteArray} concat view of two ByteArrays, uses an optional {@link ByteArrayFactory} for
 * allocation</p>
 *
 * <p>Care must be taken not to glomp output of earlier glomps, glomps are slow</p>
 */
public class Glomp extends ByteArray<ByteArray> {

	private final @Nullable ByteArrayFactory factory;

	public final ByteArray root;
	public final ByteArray suffix;

	private final int root_length;
	private final int total_length;

	public Glomp(final @Nullable ByteArrayFactory factory, final ByteArray root, final ByteArray suffix) {
		this.factory = factory;
		this.root = root;
		this.suffix = suffix;

		this.root_length = root.length();
		this.total_length = this.root_length + suffix.length();
	}

	@Override
	public byte read(final int index) {
		if (index < 0) {
			throw new IndexOutOfBoundsException("index: " + index + ", array-length: " + this.total_length);
		} else if (index < this.root_length) {
			return this.root.read(index);
		} else if (index < this.total_length) {
			return this.suffix.read(index - this.root_length);
		} else {
			throw new IndexOutOfBoundsException("index: " + index + ", array-length: " + this.total_length);
		}
	}

	@Override
	protected void internalWrite(final int index, final byte value) {
		if (index < 0) {
			throw new IndexOutOfBoundsException("index: " + index + ", array-length: " + this.total_length);
		} else if (index < this.root_length) {
			this.root.write(index, value);
		} else if (index < this.total_length) {
			this.suffix.write(index - this.root_length, value);
		} else {
			throw new IndexOutOfBoundsException("index: " + index + ", array-length: " + this.total_length);
		}
	}

	@Override
	public int length() {
		return this.total_length;
	}

	@Override
	public ByteArray copy() {
		if (this.factory == null)
			throw new IllegalStateException("Glomp was created without copying support");

		final ByteArray temp = this.factory.allocate(length());

		for (int i = 0; i < length(); i++) {
			temp.write(i, read(i));
		}

		return temp;
	}

	@Override
	protected void internalResize(final int newLength) {
		throw new UnsupportedOperationException("Can't resize glomps");
	}

	@SuppressWarnings("unchecked")
	@Override
	public ByteArray slice(final int start, final int end) {
		return new ByteArraySlice(this.factory, this, start, end);
	}
}