package engineering.taikun.distortion.structures.imp;

import engineering.taikun.distortion.SerializationUtil;
import engineering.taikun.distortion.serialization.api.ByteArray;
import engineering.taikun.distortion.serialization.imp.ArrayWrapper;
import engineering.taikun.distortion.store.api.KV;
import engineering.taikun.distortion.structures.api.DStructure;
import engineering.taikun.distortion.structures.api.DStructure.DMapStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static engineering.taikun.distortion.SerializationUtil.short_;

/**
 * <p>An implementation of a linked-hashmap. This implementation maintains a sharded size counter so that {@link #size}
 * takes constant time.</p>
 *
 * <p>Keys must be simple (not DistortionObjects) as the serialized key is used as the address. Use {@link DNavigableMap}
 * if you need object->any functionality.</p>
 *
 * <p><tt>null</tt>keys and values are allowed (though still a bad idea)</p>
 *
 * <p>This is totally synchronized (thread-safe). Iterating must synchronize on the instance.</p>
 *
 * <p>This implementation uses a number of sentinels that roughly equally subdivide the linked node chain. Double
 * linking is used to make removal faster (O(1) vs (O(n)).</p>
 *
 * <p>Performance note: Small values (25 bytes or less) are stored next to the key in the same row.</p>
 *
 * @param <K> Key type
 * @param <V> Value type
 * @param <BA> The type of the {@link ByteArray} implementation
*/
@SuppressWarnings({
		"unchecked", "ConstantConditions", "FieldAccessedSynchronizedAndUnsynchronized", "LocalVariableHidesMemberVariable"
})
public class DMap<K, V, BA extends ByteArray<BA>> extends AbstractMap<K, V> implements DMapStructure<K, V, BA> {

	public static final Object UNINIT = new Object();

	static final int INLINE_THRESHOLD = 25; // inclusive

	// the 0x00 prefix is reserved for internal uses

	static final ByteArray CONCURRENCY_KEY = new ArrayWrapper(new byte[]{ 0x00, 0x00 });
	static final ByteArray CUSTOM_KEY = new ArrayWrapper(new byte[]{ 0x00, 0x01 });
	static final ByteArray REMOTES_KEY = new ArrayWrapper(new byte[]{ 0x00, 0x02 });

	public KV<BA> kv;
	public ByteArray key;
	public final SerializationUtil<BA> util;
	public final SerializationUtil<BA>.SerializationContext context;

	// lazily initialized
	// ---

	short concurrencyLevel = -1;

	ByteArray[] SENTINEL_KEYS = null;
	ByteArray[] SIZE_KEYS = null;

	ByteArray OBJECT_PREFIX = null;

	// ---

	int modCount = 0;

	/**
	 * Non-initializing constructor
	 */
	public DMap(
			final KV<BA> kv, final ByteArray key, final SerializationUtil<BA> util,
			final SerializationUtil<BA>.SerializationContext context
	) {
		this.kv = kv;
		this.key = key;
		this.util = util;
		this.context = context;
	}

	/**
	 * Initializing constructor, must supply concurrency level
	 */
	public DMap(
			final KV<BA> kv, final ByteArray key, final SerializationUtil<BA> util,
			final SerializationUtil<BA>.SerializationContext context, final short concurrencyLevel
	) {
		this.kv = kv;
		this.key = key;
		this.util = util;
		this.context = context;

		this.concurrencyLevel = concurrencyLevel;

		this.kv.write(CONCURRENCY_KEY, util.new ShortUnion(concurrencyLevel).toBA());

		this.SENTINEL_KEYS = new ByteArray[this.concurrencyLevel];
		this.SIZE_KEYS = new ByteArray[this.concurrencyLevel];

		final byte[] keylength;
		if (concurrencyLevel <= 64) {

			keylength = util.new ShortUnion((short) 2).getBytes();

			for (int i = 0, c = 32; i < concurrencyLevel; i++, c++) {
				this.SENTINEL_KEYS[i] = new ArrayWrapper(new byte[]{ 0x00, (byte) c });
				this.SIZE_KEYS[i] = new ArrayWrapper(new byte[]{ 0x00, (byte) -c });
			}

			this.OBJECT_PREFIX = new ArrayWrapper(new byte[]{ 0x00, 0x00 });

		} else {

			keylength = util.new ShortUnion((short) 3).getBytes();

			for (int i = 0; i < concurrencyLevel; i++) {
				this.SENTINEL_KEYS[i] = util.compose(0x00, util.new ShortUnion((short) i).getBytes());
				this.SIZE_KEYS[i] = util.compose(0x00, util.new ShortUnion((short) -i).getBytes());
			}

			this.OBJECT_PREFIX = new ArrayWrapper(new byte[]{ 0x00, 0x00, 0x00 });
		}

		if (concurrencyLevel == 1) {
			// special case to point to self
			kv.write(this.SENTINEL_KEYS[0], util.compose(
							keylength, keylength, this.SENTINEL_KEYS[0], this.SENTINEL_KEYS[0]
					)
			);
		} else {
			// concurrency level at least 2
			kv.write(this.SENTINEL_KEYS[0], util.compose(
							keylength, keylength, this.SENTINEL_KEYS[1], this.SENTINEL_KEYS[concurrencyLevel - 1]
					)
			);
			for (int i = 1; i < (concurrencyLevel - 1); i++) {
				kv.write(this.SENTINEL_KEYS[i], util.compose(
								keylength, keylength, this.SENTINEL_KEYS[i+1], this.SENTINEL_KEYS[i-1]
						)
				);
			}
			kv.write(this.SENTINEL_KEYS[concurrencyLevel - 1], util.compose(
							keylength, keylength, this.SENTINEL_KEYS[0], this.SENTINEL_KEYS[concurrencyLevel - 2]
					)
			);
		}

		for (int i = 0; i < concurrencyLevel; i++) {
			kv.write(this.SIZE_KEYS[i], util.new LongUnion(0).toBA());
		}
	}

	@Override
	public synchronized KV<BA> getKV() {
		return this.kv;
	}

	@Override
	public synchronized void setKV(final KV<BA> kv) {
		this.kv = kv;
	}

	@Override
	public synchronized ByteArray getKey() {
		return this.key;
	}

	@Override
	public synchronized void setKey(final ByteArray key) {
		this.key = key;
	}

	private void completeInit() {
		if (this.concurrencyLevel == -1) {
			this.concurrencyLevel = this.util.new ShortUnion(this.kv.read(CONCURRENCY_KEY)).getShort();

			this.SENTINEL_KEYS = new ByteArray[this.concurrencyLevel];
			this.SIZE_KEYS = new ByteArray[this.concurrencyLevel];

			// 0x00, 32+ is reserved for DMap performance indexes
			// 0x00, (neg) is similarly reserved

			if (this.concurrencyLevel <= 64) {
				for (int i = 0, c = 32; i < this.concurrencyLevel; i++, c++) {
					this.SENTINEL_KEYS[i] = new ArrayWrapper(new byte[]{ 0x00, (byte) c });
					this.SIZE_KEYS[i] = new ArrayWrapper(new byte[]{ 0x00, (byte) -c });
				}
				this.OBJECT_PREFIX = new ArrayWrapper(new byte[]{ 0x00, 0x00 });
			} else {
				for (int i = 0; i < this.concurrencyLevel; i++) {
					this.SENTINEL_KEYS[i] = this.util.compose(0x00, this.util.new ShortUnion((short) i).getBytes());
					this.SIZE_KEYS[i] = this.util.compose(0x00, this.util.new ShortUnion((short) -i).getBytes());
				}
				this.OBJECT_PREFIX = new ArrayWrapper(new byte[]{ 0x00, 0x00, 0x00 });
			}
		}
	}

	/**
	 * <p>Does a very basic check to see if the passed in {@link KV} contains a DMap structure</p>
	 *
	 * <p>For an actual check, a DMap should be created and its contents iterated</p>
	 *
	 * @param kv The {@link KV} to check
	 * @return Whether or not the kv has field(s) populated that a DMap uses
	 */
	public static boolean isTouched(final KV kv) {
		return kv.read(CONCURRENCY_KEY) != null;
	}

	public synchronized short getConcurrencyLevel() {
		completeInit();
		return this.concurrencyLevel;
	}

	@Override
	public synchronized @Nullable String getCustomClass() {
		final BA fetch = this.kv.read(CUSTOM_KEY);

		if (fetch == null) {
			return null;
		}

		return (String) this.context.unpack(null, fetch);
	}

	@Override
	public synchronized void setCustomClass(final @NotNull String className) {
		this.kv.write(CUSTOM_KEY, this.context.serialize(className));
	}

	@Override
	public ByteArray getRemotesKey() {
		return REMOTES_KEY;
	}

	@Override
	public synchronized void destroy() {
		completeInit();
		clear();
		this.kv.delete(CONCURRENCY_KEY);
		this.kv.delete(CUSTOM_KEY);
		final BA temp; if ((temp = this.kv.read(REMOTES_KEY)) != null) this.context.unpackAndDestroy(this.kv, temp);
		this.kv.delete(REMOTES_KEY);

		for (final ByteArray sentinel_key : this.SENTINEL_KEYS) {
			this.kv.delete(sentinel_key);
		}
		for (final ByteArray size_key : this.SIZE_KEYS) {
			this.kv.delete(size_key);
		}
	}

	@Override
	public synchronized @NotNull Set<Entry<K, V>> entrySet() {
		completeInit();
		return new AbstractSet<Entry<K, V>>() {
			@Override
			public @NotNull Iterator<Entry<K, V>> iterator() {
				synchronized (DMap.this) {
					return new Iterator<Entry<K, V>>() {

						ByteArray cursor = DMap.this.SENTINEL_KEYS[0];

						int next_sentinel = (DMap.this.concurrencyLevel == 1) ? 0 : 1;

						ByteArray nextkey = null;
						@Nullable ByteArray deletekey = null;

						boolean fat = false;
						V value = null;

						int expectedModCount = DMap.this.modCount;

						{
							crawl();
						}

						public void crawl() {
							// given cursor and next_sentinel, increment cursor, next_sentinel, cursorvalue, and nextkey
							//  set offset
							while (true) {
								final BA cursorvalue = DMap.this.kv.read(this.cursor);

								final short nextlength
										= DMap.this.util.new ShortUnion(cursorvalue.read(0), cursorvalue.read(1)).getShort();

								this.nextkey = cursorvalue.slice(4, 4 + nextlength);

								if (this.nextkey.equals(DMap.this.SENTINEL_KEYS[0])) {
									// at end
									break;
								} else if (this.nextkey.equals(DMap.this.SENTINEL_KEYS[this.next_sentinel])) {
									// at a sentinel, need to skip it
									this.cursor = this.nextkey;

									if (this.next_sentinel == (DMap.this.concurrencyLevel - 1)) {
										this.next_sentinel = 0;
									} else {
										this.next_sentinel++;
									}
								} else {
									// at a key
									break;
								}
							}
						}

						@Override
						public boolean hasNext() {
							synchronized (DMap.this) {
								return !this.nextkey.equals(DMap.this.SENTINEL_KEYS[0]);
							}
						}

						@Override
						public Entry<K, V> next() {
							synchronized (DMap.this) {
								if (!hasNext())
									throw new NoSuchElementException();

								if (this.expectedModCount != DMap.this.modCount)
									throw new ConcurrentModificationException();

								this.cursor = this.nextkey;
								this.deletekey = this.nextkey;

								final ByteArray keybytes = this.nextkey;

								// TODO make lazy
								final K key = (K) DMap.this.context.unpack(DMap.this.kv, keybytes);

								final BA cursorvalue_capture = DMap.this.kv.read(keybytes);

								final short nextlength
										= DMap.this.util.new ShortUnion(
										cursorvalue_capture.read(0), cursorvalue_capture.read(1)).getShort();
								final short prevlength
										= DMap.this.util.new ShortUnion(
										cursorvalue_capture.read(2), cursorvalue_capture.read(3)).getShort();

								final ByteArray valuebytes;
								if (cursorvalue_capture.length() == (4 + nextlength + prevlength)) {
									valuebytes = DMap.this.kv.read(DMap.this.util.compose(keybytes, DMap.this.OBJECT_PREFIX));
									this.fat = true;
								} else {
									valuebytes = cursorvalue_capture.slice(4 + nextlength + prevlength, cursorvalue_capture.length());
									this.fat = false;
								}

								crawl();

								// TODO make lazy
								this.value = (V) DMap.this.context.unpack(DMap.this.kv, valuebytes);

								return new Entry<K, V>() {
									@Override
									@SuppressWarnings("ConstantConditions")
									public K getKey() {
										return key;
									}

									@Override
									public @Nullable V getValue() {
										return value;
									}

									@Override
									public V setValue(final V newValue) {
										synchronized (DMap.this) {
											if (deletekey == null) {
												throw new IllegalStateException();
											}

											if (expectedModCount != DMap.this.modCount)
												throw new ConcurrentModificationException();

											DMap.this.modCount++;
											expectedModCount++;

											if (fat) {
												final BA valuebytes2
														= DMap.this.kv.read(DMap.this.util.compose(keybytes, DMap.this.OBJECT_PREFIX));
												DMap.this.context.unpackAndDestroy(DMap.this.kv, valuebytes2);
											} else {
												DMap.this.context.unpackAndDestroy(
														DMap.this.kv,
														cursorvalue_capture.slice(4 + nextlength + prevlength, cursorvalue_capture.length())
												);
											}

											final @Nullable BA serialized = DMap.this.context.serialize(newValue);
											if (serialized == null) {
												DMap.this.context.store(
														DMap.this.kv, keybytes, cursorvalue_capture.slice(0, 4 + nextlength + prevlength),
														newValue
												);

												if (fat) {
													DMap.this.kv.delete(DMap.this.util.compose(keybytes, DMap.this.OBJECT_PREFIX));
												}

												fat = false;
											} else {
												if (serialized.length() > INLINE_THRESHOLD) {
													DMap.this.kv.write(DMap.this.util.compose(keybytes, DMap.this.OBJECT_PREFIX), serialized);

													if (!fat) {
														DMap.this.kv.write(
																keybytes, DMap.this.util.BA(cursorvalue_capture.slice(0, 4 + nextlength + prevlength)));
													}

													fat = true;
												} else {
													DMap.this.kv.write(
															keybytes,
															DMap.this.util.compose(
																	cursorvalue_capture.slice(0, 4 + nextlength + prevlength), serialized)
													);

													if (fat) {
														DMap.this.kv.delete(DMap.this.util.compose(keybytes, DMap.this.OBJECT_PREFIX));
													}

													fat = false;
												}
											}

											final V oldValue = value;
											value = newValue;
											return oldValue;
										}
									}

									@Override
									public String toString() {
										return key + "=" + value;
									}

									@SuppressWarnings("NonFinalFieldReferencedInHashCode")
									@Override
									public int hashCode() {
										return Objects.hashCode(key) ^ Objects.hashCode(value);
									}

									@SuppressWarnings("NonFinalFieldReferenceInEquals")
									@Override
									public boolean equals(final Object o) {
										if (o == this)
											return true;
										if (o instanceof Entry) {
											final Entry<?, ?> e = (Entry<?, ?>) o;
											if (Objects.equals(key, e.getKey()) &&
													Objects.equals(value, e.getValue()))
												return true;
										}
										return false;
									}
								};
							}
						}

						@Override
						public void remove() {
							synchronized (DMap.this) {
								if (this.deletekey == null)
									throw new IllegalStateException();

								if (this.expectedModCount != DMap.this.modCount)
									throw new ConcurrentModificationException();

								DMap.this.modCount++;
								this.expectedModCount++;

								DMap.this.remove(this.deletekey);

								this.deletekey = null;
							}
						}
					};
				}
			}

			@Override
			public int size() {
				return DMap.this.size();
			}
		};
	}

	@Override
	public synchronized boolean containsKey(final Object key) {
		final @Nullable BA headerfetch = this.kv.read(this.context.serialize(key));

		return headerfetch != null;
	}

	@Override
	public synchronized @Nullable V get(final Object key) {
		final @Nullable BA headerfetch = this.kv.read(this.context.serialize(key));

		if (headerfetch == null) {
			return null;
		}

		final short nextlength = this.util.new ShortUnion(headerfetch.read(0), headerfetch.read(1)).getShort();
		final short prevlength = this.util.new ShortUnion(headerfetch.read(2), headerfetch.read(3)).getShort();

		if (headerfetch.length() != (4 + nextlength + prevlength)) {
			return (V) this.context.unpack(this.kv, headerfetch.slice(4 + nextlength + prevlength, headerfetch.length()));
		}

		completeInit();

		final BA valuefetch = this.kv.read(this.util.compose(this.context.serialize(key), this.OBJECT_PREFIX));

		return (V) this.context.unpack(this.kv, valuefetch);
	}

	private class PutResult extends PutReflection {
		final @Nullable V v_result;

		PutResult(final DStructure reflection, final @Nullable V v_result) {
			super(null, reflection);
			this.v_result = v_result;
		}
	}

	private PutResult internalPut(final K key, final V value) {
		this.modCount++;

		final BA keybytes = this.context.serialize(key);
		final @Nullable BA headerbytes = this.kv.read(keybytes);

		if (headerbytes != null) {
			// modify existing

			final short nextlength = this.util.new ShortUnion(headerbytes.read(0), headerbytes.read(1)).getShort();
			final short prevlength = this.util.new ShortUnion(headerbytes.read(2), headerbytes.read(3)).getShort();

			final boolean fat = headerbytes.length() == (4 + nextlength + prevlength);
			final V toreturn;

			if (fat) {

				completeInit();

				final BA valuebytes = this.kv.read(this.util.compose(keybytes, this.OBJECT_PREFIX));
				toreturn = (V) this.context.unpackAndDestroy(this.kv, valuebytes);

			} else {

				toreturn = (V) this.context.unpackAndDestroy(this.kv, headerbytes.slice(4 + nextlength + prevlength, headerbytes.length()));

			}

			final @Nullable DStructure reflection;

			@Nullable final BA serialized = this.context.serialize(value);
			if (serialized == null) {
				reflection = this.context.store(this.kv, keybytes, headerbytes.slice(0, 4 + nextlength + prevlength), value);

				if (fat) {
					completeInit();

					this.kv.delete(this.util.compose(keybytes, this.OBJECT_PREFIX));
				}
			} else {
				reflection = null;

				if (serialized.length() > INLINE_THRESHOLD) {
					completeInit();

					this.kv.write(this.util.compose(keybytes, this.OBJECT_PREFIX), serialized);

					if (!fat) {
						this.kv.write(keybytes, this.util.BA(headerbytes.slice(0, 4 + nextlength + prevlength)));
					}
				} else {
					this.kv.write(keybytes, this.util.compose(headerbytes.slice(0, 4 + nextlength + prevlength), serialized));

					if (fat) {
						completeInit();

						this.kv.delete(this.util.compose(keybytes, this.OBJECT_PREFIX));
					}
				}
			}

			return new PutResult(reflection, toreturn);

		} else {
			// create new entry

			completeInit();

			final int index = ThreadLocalRandom.current().nextInt(this.concurrencyLevel);

			final ByteArray sentinel = this.SENTINEL_KEYS[index];

			// sentinel stuff
			final BA sentinel_valuebytes = this.kv.read(sentinel);

			final short sentinel_nextlength = this.util.new ShortUnion(sentinel_valuebytes.read(0), sentinel_valuebytes.read(1)).getShort();
			final short sentinel_prevlength = this.util.new ShortUnion(sentinel_valuebytes.read(2), sentinel_valuebytes.read(3)).getShort();

			final ByteArray sentinel_previous = sentinel_valuebytes.slice(
					4 + sentinel_nextlength, 4 + sentinel_nextlength + sentinel_prevlength
			);

			// sentinel previous pointer needs to point to new key
			this.kv.write(
					sentinel, this.util.compose(
							sentinel_valuebytes.read(0), sentinel_valuebytes.read(1),
							this.util.new ShortUnion(short_(keybytes.length())).getBytes(),

							sentinel_valuebytes.slice(4, 4 + sentinel_nextlength),
							keybytes
					)
			);

			// last key stuff
			final BA previous_headerbytes = this.kv.read(sentinel_previous);

			final short previous_nextlength = this.util.new ShortUnion(previous_headerbytes.read(0), previous_headerbytes.read(1)).getShort();
//			final short previous_prevlength = this.util.new ShortUnion(previous_headerbytes.read(2), previous_headerbytes.read(3)).getShort();

			// previous next pointer needs to point to new key
			this.kv.write(
					sentinel_previous, this.util.compose(
							this.util.new ShortUnion(short_(keybytes.length())).getBytes(),
							previous_headerbytes.read(2), previous_headerbytes.read(3),

							keybytes,
							previous_headerbytes.slice(4 + previous_nextlength, previous_headerbytes.length())
					)
			);

			final @Nullable DStructure reflection;

			// actual key stuff
			@Nullable final BA serialized = this.context.serialize(value);
			if (serialized == null) {

				reflection = this.context.store(
						this.kv, keybytes, this.util.compose(
								this.util.new ShortUnion(short_(sentinel.length())).getBytes(), // nextlength
								this.util.new ShortUnion(sentinel_prevlength).getBytes(), // prevlength

								sentinel, // nextkey
								sentinel_previous  // prevkey
						), value
				);

			} else {
				reflection = null;

				if (serialized.length() > INLINE_THRESHOLD) {

					this.kv.write(keybytes, this.util.compose(
									this.util.new ShortUnion(short_(sentinel.length())).getBytes(), // nextlength
									this.util.new ShortUnion(sentinel_prevlength).getBytes(), // prevlength

									sentinel, // nextkey
									sentinel_previous  // prevkey
							)
					);

					this.kv.write(this.util.compose(keybytes, this.OBJECT_PREFIX), serialized);

				} else {

					this.kv.write(keybytes, this.util.compose(
									this.util.new ShortUnion(short_(sentinel.length())).getBytes(), // nextlength
									this.util.new ShortUnion(sentinel_prevlength).getBytes(), // prevlength

									sentinel, // nextkey
									sentinel_previous,  // prevkey
									serialized // key
							)
					);

				}
			}

			// increment size
			final ByteArray sizekey = this.SIZE_KEYS[index];

			this.kv.write(sizekey, this.util.new LongUnion(this.util.new LongUnion(this.kv.read(sizekey)).getLong() + 1).toBA());

			return new PutResult(reflection, null);
		}
	}

	@Override
	public synchronized @Nullable V put(final K key, final V value) {
		return internalPut(key, value).v_result;
	}

	@Override
	public synchronized PutReflection reflectingPut(final K key, final V value) {
		return internalPut(key, value);
	}

	@Override
	public synchronized @Nullable V remove(final Object key) {
		if (containsKey(key)) {
			this.modCount++;
			final V toreturn = get(key);
			remove(this.context.serialize(key));
			return toreturn;
		} else {
			return null;
		}
	}

	void remove(final ByteArray keybytes) {

		completeInit();

		final BA headerbytes = this.kv.read(keybytes);

		final short nextlength = this.util.new ShortUnion(headerbytes.read(0), headerbytes.read(1)).getShort();
		final short prevlength = this.util.new ShortUnion(headerbytes.read(2), headerbytes.read(3)).getShort();

		final ByteArray nextkey = headerbytes.slice(4, 4 + nextlength);
		final ByteArray prevkey = headerbytes.slice(4 + nextlength, 4 + nextlength + prevlength);

		// next key manipulation

		final BA nextheader = this.kv.read(nextkey);

		final short next_nextlength = this.util.new ShortUnion(nextheader.read(0), nextheader.read(1)).getShort();
		final short next_prevlength = this.util.new ShortUnion(nextheader.read(2), nextheader.read(3)).getShort();

		// next's previous pointer needs to point to prevkey
		this.kv.write(
				nextkey, this.util.compose(
						nextheader.read(0), nextheader.read(1), // nextlength
						this.util.new ShortUnion(short_(prevkey.length())).getBytes(), // NEW prevlength

						nextheader.slice(4, 4 + next_nextlength), // nextkey
						prevkey, // NEW prevkey

						// existing key (if one exists)
						nextheader.slice(4 + next_nextlength + next_prevlength, nextheader.length())
				)
		);

		// prev key manipulation

		final BA prevheader = this.kv.read(prevkey);

		final short prev_nextlength = this.util.new ShortUnion(prevheader.read(0), prevheader.read(1)).getShort();
		final short prev_prevlength = this.util.new ShortUnion(prevheader.read(2), prevheader.read(3)).getShort();

		// previous's next pointer needs to point to nextkey
		this.kv.write(
				prevkey, this.util.compose(
						this.util.new ShortUnion(short_(nextkey.length())).getBytes(), // NEW nextlength
						prevheader.read(2), prevheader.read(3), // prevlength

						nextkey, // NEW nextkey
						prevheader.slice(4 + prev_nextlength, 4 + prev_nextlength + prev_prevlength),

						// existing key (if one exists)
						prevheader.slice(4 + prev_nextlength + prev_prevlength, prevheader.length())
				)
		);

		// destroy
		final boolean fat = headerbytes.length() == (4 + nextlength + prevlength);

		if (fat) {

			final ByteArray valueindex = this.util.compose(keybytes, this.OBJECT_PREFIX);
			final BA valuebytes = this.kv.read(valueindex);
			this.context.unpackAndDestroy(this.kv, valuebytes);
			this.kv.delete(valueindex);

		} else {

			this.context.unpackAndDestroy(this.kv, headerbytes.slice(4 + nextlength + prevlength, headerbytes.length()));

		}

		this.kv.delete(keybytes);

		// decrement size
		final ByteArray sizekey = this.SIZE_KEYS[ThreadLocalRandom.current().nextInt(this.concurrencyLevel)];

		this.kv.write(sizekey, this.util.new LongUnion(this.util.new LongUnion(this.kv.read(sizekey)).getLong() - 1).toBA());
	}

	@Override
	public synchronized int size() {
		final long truesize = mappingCount();

		if (truesize > Integer.MAX_VALUE) {
			return Integer.MAX_VALUE;
		} else {
			return (int) truesize;
		}
	}

	/**
	 * <p>This map doesn't enforce a size limit on insertions so it's possible the normal {@link #size} method will
	 * saturate</p>
	 *
	 * <p>This method will return the size as a long</p>
	 *
	 * @return The size of the map (64 bit)
	 */
	public synchronized long mappingCount() {
		completeInit();

		long accumulator = 0;

		for (final ByteArray sizekey : this.SIZE_KEYS) {
			accumulator += this.util.new LongUnion(this.kv.read(sizekey)).getLong();
		}

		return accumulator;
	}

	@Override public synchronized boolean containsValue(final Object value) {
		return super.containsValue(value);
	}

	@Override public synchronized void putAll(@NotNull final Map<? extends K, ? extends V> m) {
		super.putAll(m);
	}

	@Override public synchronized void clear() {
		super.clear();
	}

	@Override public synchronized @NotNull Set<K> keySet() {
		return super.keySet();
	}

	@Override public synchronized @NotNull Collection<V> values() {
		return super.values();
	}

	@Override public synchronized boolean isEmpty() {
		return super.isEmpty();
	}

	@Override public synchronized V getOrDefault(final Object key, final V defaultValue) {
		return super.getOrDefault(key, defaultValue);
	}

	@Override public synchronized void forEach(final BiConsumer<? super K, ? super V> action) {
		super.forEach(action);
	}

	@Override public synchronized void replaceAll(final BiFunction<? super K, ? super V, ? extends V> function) {
		super.replaceAll(function);
	}

	@Override public synchronized V putIfAbsent(final K key, final V value) {
		return super.putIfAbsent(key, value);
	}

	@Override public synchronized boolean remove(final Object key, final Object value) {
		return super.remove(key, value);
	}

	@Override public synchronized boolean replace(final K key, final V oldValue, final V newValue) {
		return super.replace(key, oldValue, newValue);
	}

	@Override public synchronized V replace(final K key, final V value) {
		return super.replace(key, value);
	}

	@Override public synchronized V computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction) {
		return super.computeIfAbsent(key, mappingFunction);
	}

	@Override
	public synchronized V computeIfPresent(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
		return super.computeIfPresent(key, remappingFunction);
	}

	@Override public synchronized V compute(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
		return super.compute(key, remappingFunction);
	}

	@Override
	public synchronized V merge(final K key, final V value, final BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
		return super.merge(key, value, remappingFunction);
	}

}
