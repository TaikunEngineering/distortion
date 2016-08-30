package engineering.taikun.distortion.serialization.api;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * <p>An abstract class representing a byte array</p>
 *
 * <p>This is meant to allow using various memory allocation techniques</p>
 *
 * <p>{@link #hashCode}, {@link #equals}, and {@link #toString} are all final (and thus stable)</p>
 *
 * @param <T> CRTP generic type
 */
public abstract class ByteArray<T extends ByteArray> implements Comparable<ByteArray> {

	/**
	 * <p>Cached hashcode value</p>
	 *
	 * <p>Defaults to 0 if needs recalculating</p>
	 */
	int hashcode = 0;

	/**
	 * Read the byte at the specified index
	 *
	 * @param index index
	 * @return a byte
	 */
	public abstract byte read(int index);

	/**
	 * Write a byte at the specified index
	 *
	 * @param index index
	 * @param value a byte
	 */
	public final void write(final int index, final byte value) {
		this.hashcode = 0;
		internalWrite(index, value);
	}

	protected abstract void internalWrite(int index, byte value);

	/**
	 * Get the length of the array
	 *
	 * @return the length
	 */
	public abstract int length();

	/**
	 * <p>Resize the array</p>
	 *
	 * <p>ByteArray slices must always throw an {@link UnsupportedOperationException}</p>
	 *
	 * <p>Other classes may or may not support this operation</p>
	 *
	 * @param newLength new length
	 */
	public final void resize(final int newLength) {
		this.hashcode = 0;
		internalResize(newLength);
	}

	protected abstract void internalResize(int newLength);

	/**
	 * <p>Allocate a copy of this ByteArray</p>
	 *
	 * <p>Arrays without a copying mechanism may return null</p>
	 *
	 * <p>The copy returned must be a full-featured copy (supports {@link #resize} if this does) and be optimally
	 * allocated (so that users who copy a slice don't keep the fat original around)</p>
	 *
	 * @return A copy
	 */
	public abstract @Nullable T copy();

	/**
	 * Create a <i>writeable</i> slice/view of this ByteArray. Slices cannot be resized.
	 *
	 * @param start The start index, inclusive
	 * @param end The end index, exclusive
	 * @return A ByteArray with the specified range
	 */
	public abstract ByteArray slice(int start, int end);

	/**
	 * <p>Return a byte[] with this ByteArray's contents, possibly mapped</p>
	 *
	 * <p>Note: This should only be used as a last resort and you should never write to the array</p>
	 *
	 * @return A byte[] with this ByteArray's contents
	 */
	public byte[] toArray() {
		final byte[] toreturn = new byte[length()];

		for (int i = 0; i < toreturn.length; i++) {
			toreturn[i] = read(i);
		}

		return toreturn;
	}

	@Override
	public final int compareTo(final @NotNull ByteArray that) {
		for (int i = 0; i < this.length() && i < that.length(); i++) {
			final int compare = Byte.compare(this.read(i), that.read(i));

			if (compare != 0)
				return compare;
		}

		// fallthrough, if beginnings are ==
		// longer array is greater

		return Integer.compare(this.length(), that.length());
	}

	public static int compare(final @NotNull ByteArray a, final @NotNull ByteArray b) {
		return a.compareTo(b);
	}

	/**
	 * Hash code using the same algorithm as Arrays::hashCode
	 *
	 * @return The hash code
	 */
	@SuppressWarnings("NonFinalFieldReferencedInHashCode")
	@Override
	public final int hashCode() {
		if (this.hashcode != 0)
			return this.hashcode;

		int toreturn = 1;
		for (int i = 0; i < length(); i++) {
			toreturn = 31 * toreturn + read(i);
		}

		this.hashcode = toreturn;

		return toreturn;
	}

	/**
	 * Equals
	 *
	 * @param o Other object to be tested
	 * @return Whether objects are equals as byte arrays
	 */
	@SuppressWarnings("NonFinalFieldReferenceInEquals")
	@Override
	public final boolean equals(final Object o) {
		if (this == o) {
			return true;
		}

		if (!(o instanceof ByteArray)) {
			return false;
		}

		final ByteArray that = (ByteArray) o;

		if (this.hashcode != 0 && that.hashcode != 0 && this.hashcode != that.hashcode) {
			return false;
		}

		final int that_length = that.length();

		if (this.length() != that_length) {
			return false;
		}

		for (int i = 0; i < that_length; i++) {
			if (this.read(i) != that.read(i)) {
				return false;
			}
		}

		return true;
	}

	/**
	 * <p>Return a String representation of the array</p>
	 *
	 * <p>I like having spaces instead of a 0-fill, so that's what I implemented here. If you hate it, don't forget you
	 * can override it in your debugger.</p>
	 *
	 * <p>Note: This uses String::format and is slow</p>
	 *
	 * @return String representation of the array
	 */
	@Override
	public final String toString() {
		final int max_index = length() - 1;

		if (max_index == -1)
			return "[]";

		final StringBuilder sb = new StringBuilder(2 + 2 * length());

		sb.append('[');

		for (int i = 0;; i++) {
			sb.append(String.format("%2X", read(i) & 0xFF));

			if (i == max_index)
				return sb.append(']').toString();
		}
	}

}
