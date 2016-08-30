package engineering.taikun.distortion.serialization.api;

/**
 * <p>An interface allowing implementations to allocate {@link ByteArray}s</p>
 */
@FunctionalInterface
public interface ByteArrayFactory<T extends ByteArray> {

	/**
	 * <p>Allocate a {@link ByteArray} of the specified length</p>
	 *
	 * <p>Exact length matters. You can internally allocate more for use with {@link ByteArray#resize} or to improve
	 * cache behavior or whatever, but you cannot expose this via {@link ByteArray#length}.</p>
	 *
	 * @param length The length of the {@link ByteArray}
	 * @return A {@link ByteArray} of appropriate length
	 */
	T allocate(int length);

}
