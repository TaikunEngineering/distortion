package engineering.taikun.distortion;

import engineering.taikun.distortion.serialization.api.ByteArrayFactory;
import engineering.taikun.distortion.serialization.imp.ArrayWrapper;
import engineering.taikun.distortion.serialization.imp.ArrayWrapperFactory;
import engineering.taikun.distortion.store.imp.DebugKV;
import engineering.taikun.distortion.structures.imp.DMap;
import org.jetbrains.annotations.NotNull;
import org.testng.annotations.Test;

import java.util.*;
import java.util.Map.Entry;

public class ConcurrentMapTest {

	static boolean locked;

	static ByteArrayFactory<ArrayWrapper> factory = new ArrayWrapperFactory();
	static SerializationUtil<ArrayWrapper> util = new SerializationUtil<>(
			factory, (short) 32,
			b -> locked = b,
			() -> locked
	);

	static LinkedHashMap<String, String> hashmap = new LinkedHashMap<>();
	static DebugKV kv = new DebugKV();
	static DMap<String, String, ArrayWrapper> dmap
			= new DMap<>(kv, SerializationUtil.EMPTY_ARRAY, util, util.new SerializationContext(kv), (short) 32);

	static ConcurrentMapHarness harness = new ConcurrentMapHarness();

	@Test
	public static void main() {
		System.out.println("Concurrent map test");

		harness.put("one", "a");

		harness.put("two", "b");

		harness.put("three", "c");

		harness.size();

		harness.containsKey("asdf");

		harness.containsKey("b");

		harness.remove("four");

		harness.get("one");

		harness.get("negative_zero");

		harness.remove("two");

		harness.put("four", "d");

		harness.remove("four");

		harness.put("five", "e");

		harness.remove("three");

		harness.put("one", "alpha");

		harness.put("six", "f");

		harness.put("five", "echo");

		harness.put("ten", "tango");

		harness.put("eleven", "zulu");

		harness.put("twelve", "sierra");

		final Set<Entry<String, String>> set1 = harness.entrySet();

		set1.size();

		final Iterator<Entry<String, String>> it1 = set1.iterator();

		dmap.destroy();

		assert_(kv.map.isEmpty());

		System.out.println("passed");
	}

	public static void assert_(final boolean bool) {
		if (!bool) throw new RuntimeException();
	}

	public static class ConcurrentMapHarness implements Map<String, String> {

		@Override
		public int size() {
			final int hashresult = hashmap.size();
			final int pyresult = dmap.size();

			assert_(hashresult == pyresult);
			assert_(hashmap.equals(dmap));
			assert_(dmap.equals(hashmap));

			return hashresult;
		}

		@Override
		public boolean isEmpty() {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean containsKey(final Object key) {
			final boolean hashresult = hashmap.containsKey(key);
			final boolean pyresult = dmap.containsKey(key);

			assert_(hashresult == pyresult);
			assert_(hashmap.equals(dmap));
			assert_(dmap.equals(hashmap));

			size();

			return hashresult;
		}

		@Override
		public boolean containsValue(final Object value) {
			throw new UnsupportedOperationException();
		}

		@Override
		public String get(final Object key) {
			final String hashresult = hashmap.get(key);
			final String pyresult = dmap.get(key);

			assert_(Objects.equals(hashresult, pyresult));
			assert_(hashmap.equals(dmap));
			assert_(dmap.equals(hashmap));

			size();

			return hashresult;
		}

		@Override
		public String put(final String key, final String value) {
			final String hashresult = hashmap.put(key, value);
			final String pyresult = dmap.put(key, value);

			assert_(Objects.equals(hashresult, pyresult));
			assert_(hashmap.equals(dmap));
			assert_(dmap.equals(hashmap));

			size();

			return hashresult;
		}

		@Override
		public String remove(final Object key) {
			final String hashresult = hashmap.remove(key);
			final String pyresult = dmap.remove(key);

			assert_(Objects.equals(hashresult, pyresult));
			assert_(hashmap.equals(dmap));
			assert_(dmap.equals(hashmap));

			size();

			return hashresult;
		}

		@Override
		public void putAll(final Map<? extends String, ? extends String> m) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void clear() {
			hashmap.clear();
			dmap.clear();

			assert_(hashmap.equals(dmap));
			assert_(dmap.equals(hashmap));

			size();
		}

		@NotNull
		@Override
		public Set<String> keySet() {
			throw new UnsupportedOperationException();
		}

		@NotNull
		@Override
		public Collection<String> values() {
			throw new UnsupportedOperationException();
		}

		@NotNull
		@Override
		public Set<Entry<String, String>> entrySet() {
			final Set<Entry<String, String>> hashresult = hashmap.entrySet();
			final Set<Entry<String, String>> pyresult = dmap.entrySet();

			return new Set<Entry<String, String>>() {
				@Override
				public int size() {
					final int hashresult2 = hashresult.size();
					final int pyresult2 = pyresult.size();

					assert_(hashresult2 == pyresult2);

					assert_(hashmap.equals(dmap));
					assert_(dmap.equals(hashmap));

					assert_(hashresult.equals(pyresult));
					assert_(pyresult.equals(hashresult));

					return hashresult2;
				}

				@Override
				public boolean isEmpty() {
					throw new UnsupportedOperationException();
				}

				@Override
				public boolean contains(final Object o) {
					throw new UnsupportedOperationException();
				}

				@NotNull
				@Override
				public Iterator<Entry<String, String>> iterator() {
					final Iterator<Entry<String, String>> hashresult2 = hashresult.iterator();
					final Iterator<Entry<String, String>> pyresult2 = pyresult.iterator();

					assert_(hashmap.equals(dmap));
					assert_(dmap.equals(hashmap));

					assert_(hashresult.equals(pyresult));
					assert_(pyresult.equals(hashresult));

					return new Iterator<Entry<String, String>>() {
						@Override
						public boolean hasNext() {
							final boolean hashresult3 = hashresult2.hasNext();
							final boolean pyresult3 = pyresult2.hasNext();

							assert_(hashresult3 == pyresult3);

							assert_(hashmap.equals(dmap));
							assert_(dmap.equals(hashmap));

							assert_(hashresult.equals(pyresult));
							assert_(pyresult.equals(hashresult));

							size();

							return hashresult3;
						}

						@Override
						public Entry<String, String> next() {
							Entry<String, String> hashresult3 = hashresult2.next();
							Entry<String, String> pyresult3 = pyresult2.next();

							assert_(hashresult3.equals(pyresult3));
							assert_(pyresult3.equals(hashresult3));

							assert_(hashmap.equals(dmap));
							assert_(dmap.equals(hashmap));

							assert_(hashresult.equals(pyresult));
							assert_(pyresult.equals(hashresult));

							size();

							return new Entry<String, String>() {
								@Override
								public String getKey() {
									String hashresult4 = hashresult3.getKey();
									String pyresult4 = pyresult3.getKey();

									assert_(hashresult4.equals(pyresult4));
									assert_(pyresult4.equals(hashresult4));

									assert_(hashresult3.equals(pyresult3));
									assert_(pyresult3.equals(hashresult3));

									assert_(hashmap.equals(dmap));
									assert_(dmap.equals(hashmap));

									assert_(hashresult.equals(pyresult));
									assert_(pyresult.equals(hashresult));

									size();

									return hashresult4;
								}

								@Override
								public String getValue() {
									String hashresult4 = hashresult3.getValue();
									String pyresult4 = pyresult3.getValue();

									assert_(hashresult4.equals(pyresult4));
									assert_(pyresult4.equals(hashresult4));

									assert_(hashresult3.equals(pyresult3));
									assert_(pyresult3.equals(hashresult3));

									assert_(hashmap.equals(dmap));
									assert_(dmap.equals(hashmap));

									assert_(hashresult.equals(pyresult));
									assert_(pyresult.equals(hashresult));

									size();

									return hashresult4;
								}

								@Override
								public String setValue(final String value) {
									String hashresult4 = hashresult3.setValue(value);
									String pyresult4 = pyresult3.setValue(value);

									assert_(hashresult4.equals(pyresult4));
									assert_(pyresult4.equals(hashresult4));

									assert_(hashresult3.equals(pyresult3));
									assert_(pyresult3.equals(hashresult3));

									assert_(hashmap.equals(dmap));
									assert_(dmap.equals(hashmap));

									assert_(hashresult.equals(pyresult));
									assert_(pyresult.equals(hashresult));

									size();

									return hashresult4;
								}
							};
						}

						@Override
						public void remove() {
							hashresult2.remove();
							pyresult2.remove();

							assert_(hashmap.equals(dmap));
							assert_(dmap.equals(hashmap));

							assert_(hashresult.equals(pyresult));
							assert_(pyresult.equals(hashresult));

							size();
						}
					};
				}

				@NotNull
				@Override
				public Object[] toArray() {
					throw new UnsupportedOperationException();
				}

				@NotNull
				@Override
				public <T> T[] toArray(final T[] a) {
					throw new UnsupportedOperationException();
				}

				@Override
				public boolean add(final Entry<String, String> stringStringEntry) {
					throw new UnsupportedOperationException();
				}

				@Override
				public boolean remove(final Object o) {
					throw new UnsupportedOperationException();
				}

				@Override
				public boolean containsAll(final Collection<?> c) {
					throw new UnsupportedOperationException();
				}

				@Override
				public boolean addAll(final Collection<? extends Entry<String, String>> c) {
					throw new UnsupportedOperationException();
				}

				@Override
				public boolean retainAll(final Collection<?> c) {
					throw new UnsupportedOperationException();
				}

				@Override
				public boolean removeAll(final Collection<?> c) {
					throw new UnsupportedOperationException();
				}

				@Override
				public void clear() {
					throw new UnsupportedOperationException();
				}
			};
		}
	}

}
