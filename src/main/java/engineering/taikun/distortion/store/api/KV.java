package engineering.taikun.distortion.store.api;

import engineering.taikun.distortion.serialization.api.ByteArray;
import org.jetbrains.annotations.Nullable;

/**
 * <p>A basic key-value interface</p>
 *
 * <p>Keys are left as {@link ByteArray}s while values are T</p>
 *
 * @param <T> A ByteArray implementation
 */
public interface KV<T extends ByteArray> {

	/**
	 * <p>Read the value as T at the specified key. Returns null if no such value.</p>
	 *
	 * <p>Throws a {@link ExpiredReadException} if the requested value is not readable because it is no longer in the
	 * buffer of historical values</p>
	 *
	 * @param key The {@link ByteArray} key
	 * @throws ExpiredReadException If the value has been dropped from the historical value buffer
	 */
	@Nullable T read(ByteArray key) throws ExpiredReadException;

	/**
	 * <p>Write a non-null value as T at the specified key</p>
	 */
	void write(ByteArray key, T value);

	/**
	 * <p>Delete the value at the specified key</p>
	 */
	void delete(ByteArray key);

	/**
	 * <p>Create another KV, using the provided {@link ByteArray} as a key prefix</p>
	 *
	 * <p>KV's represent a particular keyspace in a particular part of the global tree's state. Handling nesting data
	 * structures necessitates building up prefixes. This method gives KV implementations a chance to do things
	 * efficiently.</p>
	 *
	 * @param subkey The subkey to be used as a prefix
	 * @return A KV referencing a deeper part of the state
	 */
	KV<T> drill(ByteArray subkey);

	/**
	 * <p>Get this KV's indexing prefix, so that it is possible to determine the absolute index of items</p>
	 *
	 * @return A {@link ByteArray} of this KV's indexing prefix
	 */
	ByteArray getPrefix();

	/**
	 * A generic Exception to indicate that the read failed because the value is no longer in the buffer of historical
	 * values
	 */
	@SuppressWarnings("serial")
	class ExpiredReadException extends RuntimeException {}

}
