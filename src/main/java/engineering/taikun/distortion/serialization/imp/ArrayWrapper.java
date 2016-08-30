package engineering.taikun.distortion.serialization.imp;

import engineering.taikun.distortion.SerializationUtil;
import engineering.taikun.distortion.serialization.api.ByteArray;
import engineering.taikun.distortion.serialization.util.ByteArraySlice;

import java.util.Arrays;

/**
 * A heap-allocating, simple implementation of {@link ByteArray} using native byte arrays
 */
public class ArrayWrapper extends ByteArray<ArrayWrapper> {

	// this is globally used by a few other things
	public static final SerializationUtil<ArrayWrapper> UTIL
			= new SerializationUtil<>(new ArrayWrapperFactory(), (short) -1, null, null);

	public byte[] array;

	public ArrayWrapper(final byte[] array) {
		this.array = array;
	}

	@Override
	public byte read(final int index) {
		return this.array[index];
	}

	@Override
	protected void internalWrite(final int index, final byte value) {
		this.array[index] = value;
	}

	@Override
	public int length() {
		return this.array.length;
	}

	@Override
	protected void internalResize(final int newLength) {
		this.array = Arrays.copyOf(this.array, newLength);
	}

	@Override
	public ArrayWrapper copy() {
		return new ArrayWrapper(Arrays.copyOf(this.array, this.array.length));
	}

	@Override
	public ByteArraySlice<ArrayWrapper> slice(final int start, final int end) {
		return new ByteArraySlice<>(UTIL, this, start, end);
	}

	@Override
	public byte[] toArray() {
		return this.array;
	}

}
