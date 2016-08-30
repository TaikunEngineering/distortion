package engineering.taikun.distortion;

import engineering.taikun.distortion.SerializationUtil.SerializationContext;
import engineering.taikun.distortion.serialization.api.ByteArrayFactory;
import engineering.taikun.distortion.serialization.imp.ArrayWrapper;
import engineering.taikun.distortion.serialization.imp.ArrayWrapperFactory;
import engineering.taikun.distortion.store.imp.DebugKV;
import engineering.taikun.distortion.structures.api.DStructure;
import engineering.taikun.distortion.structures.imp.DMap;
import org.jetbrains.annotations.NotNull;
import org.testng.annotations.Test;

import java.util.*;
import java.util.Map.Entry;

public class MapTest {

	static boolean locked;

	static ByteArrayFactory<ArrayWrapper> factory = new ArrayWrapperFactory();
	static SerializationUtil<ArrayWrapper> util = new SerializationUtil<>(
			factory, (short) 1,
			b -> locked = b,
			() -> locked
	);

	@Test
	public static void main() {

		System.out.println("Map test");

		LinkedHashMap hashmap = new LinkedHashMap<>();
		DebugKV kv = new DebugKV();

		SerializationContext ctx = util.new SerializationContext(kv);

		DMap<String, Object, ArrayWrapper> dmap
				= new DMap<>(kv, SerializationUtil.EMPTY_ARRAY, util, ctx, (short) 1);

		MapHarness<String, Object> harness = new MapHarness<>(hashmap, dmap);

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

		final Set<Entry<String, Object>> set1 = harness.entrySet();

		set1.size();

		final Iterator<Entry<String, Object>> it1 = set1.iterator();
		for (int i = 0; it1.hasNext(); i++) {
			Entry<String, Object> entry = it1.next();

			entry.getKey();

			entry.getValue();

			if (i % 2 == 0) {
				entry.setValue("new value " + i);
			}

			entry.getValue();

			if (i == 3 || i == 5) {
				it1.remove();
			}
		}

		harness.clear();

		assert_(kv.map.size() == 3);

		// replacement via iterator 1

		harness.put("twenty", Collections.singletonList("asdf"));

		harness.put("twenty one", "sdfjhasdfjhkdafsjklhasdjhflkjashdflkjahsdlkfjhaldjfhalkjdshflkajhsdf");

		harness.put("twenty two", "erhj");

		final Iterator<Entry<String, Object>> it2 = harness.entrySet().iterator();
		for (int i = 0; it2.hasNext(); i++) {
			Entry<String, Object> entry = it2.next();

			entry.getKey();

			entry.getValue();

			if (i == 0) {
				entry.setValue("small");
			} else if (i == 1) {
				entry.setValue(Collections.singletonList("43kj"));
			} else {
				entry.setValue("kj432h56lkj34h6lkjh346lkjh34lk6jh34lk6jh34lkj6h34lkj6h3lkjh534l;kj45h23l;kj4h23;l");
			}
		}

		harness.clear();

		ctx.close();

		assert_(kv.map.size() == 3);

		// replacement via iterator 2

		harness.put("twenty", Collections.singletonList("kjh35"));

		harness.put("twenty one", "hjkg435nbkertm,nbdflkhjdsaf,mnaew,kjahmnw3a4,mbnw3e45,mnbawer");

		harness.put("twenty two", "3jkl54hlkj23h5lkj23h5lkj2h3lk5jh23lk5jh23lk5jh23lkj5h23lkj5h");

		final Iterator<Entry<String, Object>> it3 = harness.entrySet().iterator();
		for (int i = 0; it3.hasNext(); i++) {
			Entry<String, Object> entry = it3.next();

			entry.getKey();

			entry.getValue();

			if (i == 0) {
				entry.setValue(Collections.singletonList("jhk4352lkjh4352hkjl"));
			} else if (i == 1) {
				entry.setValue("nmcbv,mcvn,cmnb,mcvnb,mcnvb,mcnv,bmnc,vmbnc,mvnb,mcnvb,mcvn");
			} else {
				entry.setValue("lkj3");
			}
		}

		harness.clear();

		ctx.close();

		assert_(kv.map.size() == 3);

		// replacement via put 1

		harness.put("twenty", Collections.singletonList("asdf"));

		harness.put("twenty one", "sdfjhasdfjhkdafsjklhasdjhflkjashdflkjahsdlkfjhaldjfhalkjdshflkajhsdf");

		harness.put("twenty two", "erhj");

		harness.put("twenty", "small");

		harness.put("twenty one", Collections.singletonList("43kj"));

		harness.put("twenty two", "kj432h56lkj34h6lkjh346lkjh34lk6jh34lk6jh34lkj6h34lkj6h3lkjh534l;kj45h23l;kj4h23;l");

		harness.clear();

		ctx.close();

		assert_(kv.map.size() == 3);

		// replacement via put 2

		harness.put("twenty", Collections.singletonList("kjh35"));

		harness.put("twenty one", "hjkg435nbkertm,nbdflkhjdsaf,mnaew,kjahmnw3a4,mbnw3e45,mnbawer");

		harness.put("twenty two", "3jkl54hlkj23h5lkj23h5lkj2h3lk5jh23lk5jh23lk5jh23lkj5h23lkj5h");

		harness.put("twenty", Collections.singletonList("jhk4352lkjh4352hkjl"));

		harness.put("twenty one", "nmcbv,mcvn,cmnb,mcvnb,mcnvb,mcnv,bmnc,vmbnc,mvnb,mcnvb,mcvn");

		harness.put("twenty two", "lkj3");

		harness.clear();

		ctx.close();

		assert_(kv.map.size() == 3);

		// entry delete / value set testing

		dmap.put("thirty", "blah");

		Iterator<Entry<String, Object>> it4 = dmap.entrySet().iterator();
		Entry<String, Object> entry4 = it4.next();

		it4.remove();

		try {
			entry4.setValue("should not work");
			throw new RuntimeException("Failed to throw exception");
		} catch (IllegalStateException e) {}

		try {
			it4.remove();
			throw new RuntimeException("Failed to throw exception");
		} catch (IllegalStateException e) {}

		ctx.close();

		assert_(kv.map.size() == 3);

		// test cleanup

		dmap.destroy();

		ctx.close();

		assert_(kv.map.isEmpty());

		System.out.println("passed");
	}

	public static void assert_(final boolean bool) {
		if (!bool) throw new RuntimeException();
	}

	@SuppressWarnings("Duplicates")
	public static class MapHarness<K, V> implements Map<K, V> {

		public final Map<K, V> map1;
		public final Map<K, V> map2;

		public MapHarness(Map<K, V> map1, Map<K, V> map2) {
			this.map1 = map1;
			this.map2 = map2;
		}

		@Override
		public int size() {
			final int result1 = map1.size();
			final int result2 = map2.size();

			assert_(result1 == result2);
			assert_(map1.equals(map2));
			assert_(map2.equals(map1));

			return result1;
		}

		@Override
		public boolean isEmpty() {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean containsKey(final Object key) {
			final boolean result1 = map1.containsKey(key);
			final boolean result2 = map2.containsKey(key);

			assert_(result1 == result2);
			assert_(map1.equals(map2));
			assert_(map2.equals(map1));

			size();

			return result1;
		}

		@Override
		public boolean containsValue(final Object value) {
			throw new UnsupportedOperationException();
		}

		@Override
		public V get(final Object key) {
			final V result1 = map1.get(key);
			final V result2 = map2.get(key);

			assert_(Objects.equals(result1, result2));
			assert_(map1.equals(map2));
			assert_(map2.equals(map1));

			size();

			return result1;
		}

		@Override
		public V put(final K key, final V value) {
			final V result1 = map1.put(key, value);
			final V result2 = map2.put(key, value);

			if (!(result2 instanceof DStructure)) {
				assert_(Objects.equals(result1, result2));
				assert_(Objects.equals(result2, result1));
			}

			assert_(map1.equals(map2));
			assert_(map2.equals(map1));

			size();

			return result1;
		}

		@Override
		public V remove(final Object key) {
			final V result1 = map1.remove(key);
			final V result2 = map2.remove(key);

			assert_(Objects.equals(result1, result2));
			assert_(map1.equals(map2));
			assert_(map2.equals(map1));

			size();

			return result1;
		}

		@Override
		public void putAll(final Map<? extends K, ? extends V> m) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void clear() {
			map1.clear();
			map2.clear();

			assert_(map1.equals(map2));
			assert_(map2.equals(map1));

			size();
		}

		@NotNull
		@Override
		public Set<K> keySet() {
			throw new UnsupportedOperationException();
		}

		@NotNull
		@Override
		public Collection<V> values() {
			throw new UnsupportedOperationException();
		}

		@NotNull
		@Override
		public Set<Entry<K, V>> entrySet() {
			final Set<Entry<K, V>> result1 = map1.entrySet();
			final Set<Entry<K, V>> result2 = map2.entrySet();

			return new Set<Entry<K, V>>() {
				@Override
				public int size() {
					final int result12 = result1.size();
					final int result22 = result2.size();

					assert_(result12 == result22);

					assert_(map1.equals(map2));
					assert_(map2.equals(map1));

					assert_(result1.equals(result2));
					assert_(result2.equals(result1));

					return result12;
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
				public Iterator<Entry<K, V>> iterator() {
					final Iterator<Entry<K, V>> result12 = result1.iterator();
					final Iterator<Entry<K, V>> result22 = result2.iterator();

					assert_(map1.equals(map2));
					assert_(map2.equals(map1));

					assert_(result1.equals(result2));
					assert_(result2.equals(result1));

					return new Iterator<Entry<K, V>>() {
						@Override
						public boolean hasNext() {
							final boolean result13 = result12.hasNext();
							final boolean result23 = result22.hasNext();

							assert_(result13 == result23);

							assert_(map1.equals(map2));
							assert_(map2.equals(map1));

							assert_(result1.equals(result2));
							assert_(result2.equals(result1));

							size();

							return result13;
						}

						@Override
						public Entry<K, V> next() {
							Entry<K, V> result13 = result12.next();
							Entry<K, V> result23 = result22.next();

							assert_(result13.equals(result23));
							assert_(result23.equals(result13));

							assert_(map1.equals(map2));
							assert_(map2.equals(map1));

							assert_(result1.equals(result2));
							assert_(result2.equals(result1));

							size();

							return new Entry<K, V>() {
								@Override
								public K getKey() {
									K result14 = result13.getKey();
									K result24 = result23.getKey();

									assert_(result14.equals(result24));
									assert_(result24.equals(result14));

									assert_(result13.equals(result23));
									assert_(result23.equals(result13));

									assert_(map1.equals(map2));
									assert_(map2.equals(map1));

									assert_(result1.equals(result2));
									assert_(result2.equals(result1));

									size();

									return result14;
								}

								@Override
								public V getValue() {
									V result14 = result13.getValue();
									V result24 = result23.getValue();

									assert_(result14.equals(result24));
									assert_(result24.equals(result14));

									assert_(result13.equals(result23));
									assert_(result23.equals(result13));

									assert_(map1.equals(map2));
									assert_(map2.equals(map1));

									assert_(result1.equals(result2));
									assert_(result2.equals(result1));

									size();

									return result14;
								}

								@Override
								public V setValue(final V value) {
									V result14 = result13.setValue(value);
									V result24 = result23.setValue(value);

									if (!(result24 instanceof DStructure)) {
										assert_(result14.equals(result24));
										assert_(result24.equals(result14));
									}

									assert_(result13.equals(result23));
									assert_(result23.equals(result13));

									assert_(map1.equals(map2));
									assert_(map2.equals(map1));

									assert_(result1.equals(result2));
									assert_(result2.equals(result1));

									size();

									return result14;
								}
							};
						}

						@Override
						public void remove() {
							result12.remove();
							result22.remove();

							assert_(map1.equals(map2));
							assert_(map2.equals(map1));

							assert_(result1.equals(result2));
							assert_(result2.equals(result1));

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
				public boolean add(final Entry<K, V> entry) {
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
				public boolean addAll(final Collection<? extends Entry<K, V>> c) {
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
