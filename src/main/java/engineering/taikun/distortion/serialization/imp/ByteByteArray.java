package engineering.taikun.distortion.serialization.imp;

import engineering.taikun.distortion.serialization.api.ByteArray;
import engineering.taikun.distortion.serialization.util.ByteArraySlice;
import org.jetbrains.annotations.Nullable;

public class ByteByteArray extends ByteArray<ByteByteArray> {

	public byte value;

	public ByteByteArray(final byte value) {
		this.value = value;
	}

	@Override
	public byte read(final int index) {
		if (index != 0) {
			throw new IndexOutOfBoundsException();
		}

		return this.value;
	}

	@Override protected void internalWrite(final int index, final byte value) {
		if (index != 0) {
			throw new IndexOutOfBoundsException();
		}

		this.value = value;
	}

	@Override public int length() {
		return 1;
	}

	@Override protected void internalResize(final int newLength) {
		throw new UnsupportedOperationException();
	}

	@Nullable @Override public ByteByteArray copy() {
		return new ByteByteArray(this.value);
	}

	@Override public ByteArray slice(final int start, final int end) {
		return new ByteArraySlice<>(null, this, start, end);
	}
}
