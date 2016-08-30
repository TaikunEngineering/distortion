package engineering.taikun.distortion;

import engineering.taikun.distortion.SerializationUtil.SerializationContext;
import engineering.taikun.distortion.serialization.api.ByteArrayFactory;
import engineering.taikun.distortion.serialization.imp.ArrayWrapper;
import engineering.taikun.distortion.serialization.imp.ArrayWrapperFactory;
import engineering.taikun.distortion.store.imp.DebugKV;
import engineering.taikun.distortion.structures.api.DStructure;
import engineering.taikun.distortion.structures.imp.DNavigableMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.testng.annotations.Test;

import java.util.*;
import java.util.Map.Entry;

@SuppressWarnings("UnqualifiedFieldAccess")
public class NavigableMapTest {

	static boolean locked;

	static ByteArrayFactory<ArrayWrapper> factory = new ArrayWrapperFactory();
	static SerializationUtil<ArrayWrapper> util = new SerializationUtil<>(
			factory, (short) 1,
			b -> locked = b,
			() -> locked
	);

	@Test(invocationCount = 1000)
	public static void main() {

		System.out.println("Sorted map test");

		final TreeMap<String, Object> treemap = new TreeMap<>();
		final DebugKV kv = new DebugKV();

		SerializationContext ctx = util.new SerializationContext(kv);

		final DNavigableMap<String, Object, ArrayWrapper> dmap = new DNavigableMap<>(
				kv, SerializationUtil.EMPTY_ARRAY, util, ctx, (short) 4, null
		);

		final NavigableMapHarness<String, Object> harness = new NavigableMapHarness<>(treemap, dmap);

		dmap.integrityCheck();

		harness.put("one", "a");
		dmap.integrityCheck();

		harness.put("two", "b");
		dmap.integrityCheck();

		harness.put("three", "c");
		dmap.integrityCheck();

		harness.size();
		dmap.integrityCheck();

		harness.containsKey("asdf");
		dmap.integrityCheck();

		harness.containsKey("b");
		dmap.integrityCheck();

		harness.remove("four");
		dmap.integrityCheck();

		harness.get("one");
		dmap.integrityCheck();

		harness.get("negative_zero");
		dmap.integrityCheck();

		harness.remove("two");
		dmap.integrityCheck();

		harness.put("four", "d");
		dmap.integrityCheck();

		harness.remove("four");
		dmap.integrityCheck();

		harness.put("five", "e");
		dmap.integrityCheck();

		harness.remove("three");
		dmap.integrityCheck();

		harness.put("one", "alpha");
		dmap.integrityCheck();

		harness.put("six", "f");
		dmap.integrityCheck();

		harness.put("five", "echo");
		dmap.integrityCheck();

		harness.put("ten", "tango");
		dmap.integrityCheck();

		harness.put("eleven", "zulu");
		dmap.integrityCheck();

		harness.put("twelve", "sierra");
		dmap.integrityCheck();

		final Set<Entry<String, Object>> set1 = harness.entrySet();
		dmap.integrityCheck();

		set1.size();
		dmap.integrityCheck();

		final Iterator<Entry<String, Object>> it1 = set1.iterator();
		dmap.integrityCheck();
		for (int i = 0; it1.hasNext(); i++) {
			final Entry<String, Object> entry = it1.next();
			dmap.integrityCheck();

			entry.getKey();
			dmap.integrityCheck();

			entry.getValue();
			dmap.integrityCheck();

			if (i % 2 == 0) {
				entry.setValue("new value " + i);
				dmap.integrityCheck();
			}

			entry.getValue();
			dmap.integrityCheck();

			if (i == 3 || i == 5) {
				it1.remove();
				dmap.integrityCheck();
			}
		}

		harness.clear();
		dmap.integrityCheck();

		assert_(kv.map.size() == 3);

		// replacement via iterator 1

		harness.put("twenty", Collections.singletonList("asdf"));
		dmap.integrityCheck();

		harness.put("twenty one", "sdfjhasdfjhkdafsjklhasdjhflkjashdflkjahsdlkfjhaldjfhalkjdshflkajhsdf");
		dmap.integrityCheck();

		harness.put("twenty two", "erhj");
		dmap.integrityCheck();

		final Iterator<Entry<String, Object>> it2 = harness.entrySet().iterator();
		dmap.integrityCheck();
		for (int i = 0; it2.hasNext(); i++) {
			final Entry<String, Object> entry = it2.next();
			dmap.integrityCheck();

			entry.getKey();
			dmap.integrityCheck();

			entry.getValue();
			dmap.integrityCheck();

			if (i == 0) {
				entry.setValue("small");
				dmap.integrityCheck();
			} else if (i == 1) {
				entry.setValue(Collections.singletonList("43kj"));
				dmap.integrityCheck();
			} else {
				entry.setValue("kj432h56lkj34h6lkjh346lkjh34lk6jh34lk6jh34lkj6h34lkj6h3lkjh534l;kj45h23l;kj4h23;l");
				dmap.integrityCheck();
			}
		}

		harness.clear();
		dmap.integrityCheck();

		ctx.close();

		assert_(kv.map.size() == 3);

		// replacement via iterator 2

		harness.put("twenty", Collections.singletonList("kjh35"));
		dmap.integrityCheck();

		harness.put("twenty one", "hjkg435nbkertm,nbdflkhjdsaf,mnaew,kjahmnw3a4,mbnw3e45,mnbawer");
		dmap.integrityCheck();

		harness.put("twenty two", "3jkl54hlkj23h5lkj23h5lkj2h3lk5jh23lk5jh23lk5jh23lkj5h23lkj5h");
		dmap.integrityCheck();

		final Iterator<Entry<String, Object>> it3 = harness.entrySet().iterator();
		dmap.integrityCheck();
		for (int i = 0; it3.hasNext(); i++) {
			final Entry<String, Object> entry = it3.next();
			dmap.integrityCheck();

			entry.getKey();
			dmap.integrityCheck();

			entry.getValue();
			dmap.integrityCheck();

			if (i == 0) {
				entry.setValue(Collections.singletonList("jhk4352lkjh4352hkjl"));
				dmap.integrityCheck();
			} else if (i == 1) {
				entry.setValue("nmcbv,mcvn,cmnb,mcvnb,mcnvb,mcnv,bmnc,vmbnc,mvnb,mcnvb,mcvn");
				dmap.integrityCheck();
			} else {
				entry.setValue("lkj3");
				dmap.integrityCheck();
			}
		}

		harness.clear();
		dmap.integrityCheck();

		ctx.close();

		assert_(kv.map.size() == 3);

		// replacement via put 1

		harness.put("twenty", Collections.singletonList("asdf"));
		dmap.integrityCheck();

		harness.put("twenty one", "sdfjhasdfjhkdafsjklhasdjhflkjashdflkjahsdlkfjhaldjfhalkjdshflkajhsdf");
		dmap.integrityCheck();

		harness.put("twenty two", "erhj");
		dmap.integrityCheck();

		harness.put("twenty", "small");
		dmap.integrityCheck();

		harness.put("twenty one", Collections.singletonList("43kj"));
		dmap.integrityCheck();

		harness.put("twenty two", "kj432h56lkj34h6lkjh346lkjh34lk6jh34lk6jh34lkj6h34lkj6h3lkjh534l;kj45h23l;kj4h23;l");
		dmap.integrityCheck();

		harness.clear();
		dmap.integrityCheck();

		ctx.close();

		assert_(kv.map.size() == 3);

		// replacement via put 2

		harness.put("twenty", Collections.singletonList("kjh35"));
		dmap.integrityCheck();

		harness.put("twenty one", "hjkg435nbkertm,nbdflkhjdsaf,mnaew,kjahmnw3a4,mbnw3e45,mnbawer");
		dmap.integrityCheck();

		harness.put("twenty two", "3jkl54hlkj23h5lkj23h5lkj2h3lk5jh23lk5jh23lk5jh23lkj5h23lkj5h");
		dmap.integrityCheck();

		harness.put("twenty", Collections.singletonList("jhk4352lkjh4352hkjl"));
		dmap.integrityCheck();

		harness.put("twenty one", "nmcbv,mcvn,cmnb,mcvnb,mcnvb,mcnv,bmnc,vmbnc,mvnb,mcnvb,mcvn");
		dmap.integrityCheck();

		harness.put("twenty two", "lkj3");
		dmap.integrityCheck();

		harness.clear();
		dmap.integrityCheck();

		ctx.close();

		assert_(kv.map.size() == 3);

		// navigation tests

		harness.put("f", 1);
//			harness.put("g", 2);
		harness.put("h", 3);
//			harness.put("i", 4);
		harness.put("j", 5);
//			harness.put("k", 6);
		harness.put("l", 7);
//			harness.put("m", 8);
		harness.put("n", 9);

		/// lower

		harness.lowerEntry("a");
		dmap.integrityCheck();

		harness.lowerEntry("f");
		dmap.integrityCheck();

		harness.lowerEntry("k");
		dmap.integrityCheck();

		harness.lowerKey("a");
		dmap.integrityCheck();

		harness.lowerKey("f");
		dmap.integrityCheck();

		harness.lowerKey("k");
		dmap.integrityCheck();

		/// floor

		harness.floorEntry("a");
		dmap.integrityCheck();

		harness.floorEntry("f");
		dmap.integrityCheck();

		harness.floorEntry("k");
		dmap.integrityCheck();

		harness.floorKey("a");
		dmap.integrityCheck();

		harness.floorKey("f");
		dmap.integrityCheck();

		harness.floorKey("k");
		dmap.integrityCheck();

		/// ceiling

		harness.ceilingEntry("z");
		dmap.integrityCheck();

		harness.ceilingEntry("n");
		dmap.integrityCheck();

		harness.ceilingEntry("i");
		dmap.integrityCheck();

		harness.ceilingKey("z");
		dmap.integrityCheck();

		harness.ceilingKey("n");
		dmap.integrityCheck();

		harness.ceilingKey("i");
		dmap.integrityCheck();

		/// higher

		harness.higherEntry("z");
		dmap.integrityCheck();

		harness.higherEntry("n");
		dmap.integrityCheck();

		harness.higherEntry("i");
		dmap.integrityCheck();

		harness.higherKey("z");
		dmap.integrityCheck();

		harness.higherKey("n");
		dmap.integrityCheck();

		harness.higherKey("i");
		dmap.integrityCheck();

		/// first/last

		harness.firstEntry();
		dmap.integrityCheck();

		harness.firstKey();
		dmap.integrityCheck();

		harness.lastEntry();
		dmap.integrityCheck();

		harness.lastKey();
		dmap.integrityCheck();

		/// first/last removal

		Entry<String, Object> first_entry = harness.pollFirstEntry();
		dmap.integrityCheck();

		try {
			((EntryHarness) first_entry).result2.setValue("entry removed");
			throw new RuntimeException();
		} catch (IllegalStateException e) {}
		dmap.integrityCheck();

		harness.pollLastEntry();
		dmap.integrityCheck();

		harness.clear();
		dmap.integrityCheck();

		assert_(kv.map.size() == 3);

		/// single element removal

		harness.put("single", "element");

		harness.pollFirstEntry();
		dmap.integrityCheck();

		harness.put("two", "tests");

		harness.pollLastEntry();
		dmap.integrityCheck();

		/// non removals (and a lastnode)

		assert_(kv.map.size() == 3);

		harness.pollFirstEntry();
		dmap.integrityCheck();

		harness.pollLastEntry();
		dmap.integrityCheck();

		harness.lastEntry();
		dmap.integrityCheck();

		// entry delete / value set testing

		dmap.put("thirty", "blah");
		dmap.integrityCheck();

		final Iterator<Entry<String, Object>> it4 = dmap.entrySet().iterator();
		dmap.integrityCheck();
		final Entry<String, Object> entry4 = it4.next();
		dmap.integrityCheck();

		it4.remove();
		dmap.integrityCheck();

		try {
			entry4.setValue("should not work");
			throw new RuntimeException("Failed to throw exception");
		} catch (final IllegalStateException e) {}
		dmap.integrityCheck();

		try {
			it4.remove();
			throw new RuntimeException("Failed to throw exception");
		} catch (final IllegalStateException e) {}
		dmap.integrityCheck();

		assert_(kv.map.size() == 3);

		// entryset iteration

		dmap.put("gummy", "worm");
		dmap.put("red", "yellow");
		dmap.put("green", "blue");
		dmap.put("blue", "red");

		final Iterator<Entry<String, Object>> it5 = dmap.entrySet().iterator();
		dmap.integrityCheck();

		it5.next();
		it5.next();
		it5.next();
		it5.next();

		try {
			it5.next();
			throw new RuntimeException("Failed to throw exception");
		} catch (final NoSuchElementException e) {}

//		final Iterator<Entry<String, Object>> it6 = dmap.entrySet().iterator();
//		dmap.put("yellow", "red");
//		try {
//			it6.next();
//			throw new RuntimeException("Failed to the throw exception");
//		} catch (final ConcurrentModificationException e) {}
//
//		final Iterator<Entry<String, Object>> it7 = dmap.entrySet().iterator();
//		final Entry<String, Object> entry7 = it7.next();
//		dmap.put("blue", "green");
//		try {
//			entry7.setValue("cornstarch");
//			throw new RuntimeException("Failed to the throw exception");
//		} catch (final ConcurrentModificationException e) {}
//
//		final Iterator<Entry<String, Object>> it8 = dmap.entrySet().iterator();
//		it8.next();
//		dmap.put("red", "blue");
//		try {
//			it8.remove();
//			throw new RuntimeException("Failed to the throw exception");
//		} catch (final ConcurrentModificationException e) {}

		// test cleanup

		dmap.destroy();

		ctx.close();

		assert_(kv.map.isEmpty());

		System.out.println("passed");
	}

	public static void assert_(final boolean bool) {
		if (!bool) throw new RuntimeException();
	}

	public static boolean classEq(final Object a, final Object b) {
		if (a == null && b == null) {
			return true;
		} else if (a == null || b == null) {
			return false;
		} else {
			return a.getClass().equals(b.getClass());
		}
	}

	public static void SortedEq(final Map a, final Map b) {
		Iterator it1 = a.entrySet().iterator(), it2 = b.entrySet().iterator();

		while (it1.hasNext() && it2.hasNext()) {
			assert_(Objects.equals(it1.next(), it2.next()));
		}

		assert_(!it1.hasNext());
		assert_(!it2.hasNext());
	}

	public static void SortedEq(final Collection a, final Collection b) {
		Iterator it1 = a.iterator(), it2 = b.iterator();

		while (it1.hasNext() && it2.hasNext()) {
			assert_(Objects.equals(it1.next(), it2.next()));
		}

		assert_(!it1.hasNext());
		assert_(!it2.hasNext());
	}

	public static class SortedMapHarness<K, V> implements SortedMap<K, V> {

		public final NavigableMap<K, V> map1;
		public final NavigableMap<K, V> map2;

		public final SortedMap<K, V> smap1;
		public final SortedMap<K, V> smap2;

		public SortedMapHarness(
				final NavigableMap<K, V> map1, final NavigableMap<K, V> map2,
				final SortedMap<K, V> smap1, final SortedMap<K, V> smap2
		) {
			this.map1 = map1;
			this.map2 = map2;

			this.smap1 = smap1;
			this.smap2 = smap2;
		}

		@Override public Comparator<? super K> comparator() {
			throw new UnsupportedOperationException();
		}

		@Override public SortedMap<K, V> subMap(final K fromKey, final K toKey) {
			final SortedMap<K, V> result1 = map1.subMap(fromKey, toKey);
			final SortedMap<K, V> result2 = map2.subMap(fromKey, toKey);

			SortedEq(result1, result2);

			SortedEq(map1, map2);

			size();

			return new SortedMapHarness<>(map1, map2, result1, result2);
		}

		@Override public SortedMap<K, V> headMap(final K toKey) {
			final SortedMap<K, V> result1 = map1.headMap(toKey);
			final SortedMap<K, V> result2 = map2.headMap(toKey);

			SortedEq(result1, result2);

			SortedEq(map1, map2);

			size();

			return new SortedMapHarness<>(map1, map2, result1, result2);
		}

		@Override public SortedMap<K, V> tailMap(final K fromKey) {
			final SortedMap<K, V> result1 = map1.tailMap(fromKey);
			final SortedMap<K, V> result2 = map2.tailMap(fromKey);

			SortedEq(result1, result2);

			SortedEq(map1, map2);

			size();

			return new SortedMapHarness<>(map1, map2, result1, result2);
		}

		@Override public K firstKey() {
			final K result1 = map1.firstKey();
			final K result2 = map2.firstKey();

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public K lastKey() {
			final K result1 = map1.lastKey();
			final K result2 = map2.lastKey();

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public int size() {
			final int result1 = map1.size();
			final int result2 = map2.size();

			assert_(result1 == result2);

			SortedEq(map1, map2);

			return result1;
		}

		@Override public boolean isEmpty() {
			throw new UnsupportedOperationException();
		}

		@Override public boolean containsKey(final Object key) {
			final boolean result1 = map1.containsKey(key);
			final boolean result2 = map2.containsKey(key);

			assert_(result1 == result2);

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public boolean containsValue(final Object value) {
			throw new UnsupportedOperationException();
		}

		@Override public V get(final Object key) {
			final V result1 = map1.get(key);
			final V result2 = map2.get(key);

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public V put(final K key, final V value) {
			final V result1 = map1.put(key, value);
			final V result2 = map2.put(key, value);

			if (!(result2 instanceof DStructure)) {
				assert_(Objects.equals(result1, result2));
				assert_(Objects.equals(result2, result1));
			}

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public V remove(final Object key) {
			final V result1 = map1.remove(key);
			final V result2 = map2.remove(key);

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public void putAll(final Map<? extends K, ? extends V> m) {
			throw new UnsupportedOperationException();
		}

		@Override public void clear() {
			map1.clear();
			map2.clear();

			SortedEq(map1, map2);

			size();
		}

		@Override public @NotNull Set<K> keySet() {
			final Set<K> result1 = map1.keySet();
			final Set<K> result2 = map2.keySet();

			SortedEq(result1, result2);

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public @NotNull Collection<V> values() {
			final Collection<V> result1 = map1.values();
			final Collection<V> result2 = map2.values();

			SortedEq(result1, result2);

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public @NotNull Set<Entry<K, V>> entrySet() {
			final Set<Entry<K, V>> result1 = map1.entrySet();
			final Set<Entry<K, V>> result2 = map2.entrySet();

			return new Set<Entry<K, V>>() {
				@Override
				public int size() {
					final int result12 = result1.size();
					final int result22 = result2.size();

					assert_(result12 == result22);

					SortedEq(result1, result2);

					SortedEq(map1, map2);

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

					SortedEq(result1, result2);

					SortedEq(map1, map2);

					return new Iterator<Entry<K, V>>() {
						@Override
						public boolean hasNext() {
							final boolean result13 = result12.hasNext();
							final boolean result23 = result22.hasNext();

							assert_(result13 == result23);

							SortedEq(result1, result2);

							SortedEq(map1, map2);

							size();

							return result13;
						}

						@Override
						public Entry<K, V> next() {
							final Entry<K, V> result13 = result12.next();
							final Entry<K, V> result23 = result22.next();

							assert_(result13.equals(result23));
							assert_(result23.equals(result13));

							SortedEq(result1, result2);

							SortedEq(map1, map2);

							size();

							return new Entry<K, V>() {
								@Override
								public K getKey() {
									final K result14 = result13.getKey();
									final K result24 = result23.getKey();

									assert_(result14.equals(result24));
									assert_(result24.equals(result14));

									assert_(result13.equals(result23));
									assert_(result23.equals(result13));

									SortedEq(result1, result2);

									SortedEq(map1, map2);

									size();

									return result14;
								}

								@Override
								public V getValue() {
									final V result14 = result13.getValue();
									final V result24 = result23.getValue();

									assert_(result14.equals(result24));
									assert_(result24.equals(result14));

									assert_(result13.equals(result23));
									assert_(result23.equals(result13));

									SortedEq(result1, result2);

									SortedEq(map1, map2);

									size();

									return result14;
								}

								@Override
								public V setValue(final V value) {
									final V result14 = result13.setValue(value);
									final V result24 = result23.setValue(value);

									if (!(result24 instanceof DStructure)) {
										assert_(result14.equals(result24));
										assert_(result24.equals(result14));
									}

									assert_(result13.equals(result23));
									assert_(result23.equals(result13));

									SortedEq(result1, result2);

									SortedEq(map1, map2);

									size();

									return result14;
								}
							};
						}

						@Override
						public void remove() {
							result12.remove();
							result22.remove();

							SortedEq(result1, result2);

							SortedEq(map1, map2);

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

	@SuppressWarnings("UnqualifiedFieldAccess")
	public static class NavigableMapHarness<K, V> implements NavigableMap<K, V> {

		public final NavigableMap<K, V> map1;
		public final NavigableMap<K, V> map2;

		public NavigableMapHarness(final NavigableMap<K, V> map1, final NavigableMap<K, V> map2) {
			this.map1 = map1;
			this.map2 = map2;
		}

		@Override public Entry<K, V> lowerEntry(final K key) {
			final Entry<K, V> result1 = map1.lowerEntry(key);
			final Entry<K, V> result2 = map2.lowerEntry(key);

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return new EntryHarness(map1, map2, result1, result2);
		}

		@Override public K lowerKey(final K key) {
			final K result1 = map1.lowerKey(key);
			final K result2 = map2.lowerKey(key);

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public Entry<K, V> floorEntry(final K key) {
			final Entry<K, V> result1 = map1.floorEntry(key);
			final Entry<K, V> result2 = map2.floorEntry(key);

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return new EntryHarness(map1, map2, result1, result2);
		}

		@Override public K floorKey(final K key) {
			final K result1 = map1.floorKey(key);
			final K result2 = map2.floorKey(key);

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public Entry<K, V> ceilingEntry(final K key) {
			final Entry<K, V> result1 = map1.ceilingEntry(key);
			final Entry<K, V> result2 = map2.ceilingEntry(key);

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return new EntryHarness(map1, map2, result1, result2);
		}

		@Override public K ceilingKey(final K key) {
			final K result1 = map1.ceilingKey(key);
			final K result2 = map2.ceilingKey(key);

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public Entry<K, V> higherEntry(final K key) {
			final Entry<K, V> result1 = map1.higherEntry(key);
			final Entry<K, V> result2 = map2.higherEntry(key);

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return new EntryHarness(map1, map2, result1, result2);
		}

		@Override public K higherKey(final K key) {
			final K result1 = map1.higherKey(key);
			final K result2 = map2.higherKey(key);

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public Entry<K, V> firstEntry() {
			final Entry<K, V> result1 = map1.firstEntry();
			final Entry<K, V> result2 = map2.firstEntry();

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return new EntryHarness(map1, map2, result1, result2);
		}

		@Override public Entry<K, V> lastEntry() {
			final Entry<K, V> result1 = map1.lastEntry();
			final Entry<K, V> result2 = map2.lastEntry();

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return new EntryHarness(map1, map2, result1, result2);
		}

		@Override public Entry<K, V> pollFirstEntry() {
			final Entry<K, V> result1 = map1.pollFirstEntry();
			final Entry<K, V> result2 = map2.pollFirstEntry();

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return new EntryHarness(map1, map2, result1, result2);
		}

		@Override public Entry<K, V> pollLastEntry() {
			final Entry<K, V> result1 = map1.pollLastEntry();
			final Entry<K, V> result2 = map2.pollLastEntry();

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return new EntryHarness(map1, map2, result1, result2);
		}

		@Override public NavigableMap<K, V> descendingMap() {
			final NavigableMap<K, V> result1 = map1.descendingMap();
			final NavigableMap<K, V> result2 = map2.descendingMap();

			SortedEq(result1, result2);

			SortedEq(map1, map2);

			size();

			return new NavigableMapHarness<>(result1, result2);
		}

		@Override public NavigableSet<K> navigableKeySet() {
			final NavigableSet<K> result1 = map1.navigableKeySet();
			final NavigableSet<K> result2 = map2.navigableKeySet();

			SortedEq(result1, result2);

			SortedEq(map1, map2);

			size();

			return new NavigableSetHarness(map1, map2, result1, result2);
		}

		@Override public NavigableSet<K> descendingKeySet() {
			final NavigableSet<K> result1 = map1.descendingKeySet();
			final NavigableSet<K> result2 = map2.descendingKeySet();

			SortedEq(result1, result2);

			SortedEq(map1, map2);

			size();

			return new NavigableSetHarness(map1, map2, result1, result2);
		}

		@Override
		public NavigableMap<K, V> subMap(
				final K fromKey, final boolean fromInclusive, final K toKey, final boolean toInclusive
		) {
			NavigableMap<K, V> result1 = null;
			NavigableMap<K, V> result2 = null;

			Exception e1 = null;
			Exception e2 = null;

			try {
				result1 = map1.subMap(fromKey, fromInclusive, toKey, toInclusive);
			} catch (Exception e) {
				e1 = e;
			}

			try {
				result2 = map2.subMap(fromKey, fromInclusive, toKey, toInclusive);
			} catch (Exception e) {
				e2 = e;
			}

			assert_(classEq(e1, e2));

			if (e1 != null) {
				return null;
			}

			SortedEq(result1, result2);

			SortedEq(map1, map2);

			size();

			return new NavigableMapHarness<>(result1, result2);
		}

		@Override public NavigableMap<K, V> headMap(final K toKey, final boolean inclusive) {
			NavigableMap<K, V> result1 = null;
			NavigableMap<K, V> result2 = null;

			Exception e1 = null;
			Exception e2 = null;

			try {
				result1 = map1.headMap(toKey, inclusive);
			} catch (Exception e) {
				e1 = e;
			}

			try {
				result2 = map2.headMap(toKey, inclusive);
			} catch (Exception e) {
				e2 = e;
			}

			assert_(classEq(e1, e2));

			if (e1 != null) {
				return null;
			}

			SortedEq(result1, result2);

			SortedEq(map1, map2);

			size();

			return new NavigableMapHarness<>(result1, result2);
		}

		@Override public NavigableMap<K, V> tailMap(final K fromKey, final boolean inclusive) {
			NavigableMap<K, V> result1 = null;
			NavigableMap<K, V> result2 = null;

			Exception e1 = null;
			Exception e2 = null;

			try {
				result1 = map1.tailMap(fromKey, inclusive);
			} catch (Exception e) {
				e1 = e;
			}

			try {
				result2 = map2.tailMap(fromKey, inclusive);
			} catch (Exception e) {
				e2 = e;
			}

			assert_(classEq(e1, e2));

			if (e1 != null) {
				return null;
			}

			SortedEq(result1, result2);

			SortedEq(map1, map2);

			size();

			return new NavigableMapHarness<>(result1, result2);
		}

		@Override public Comparator<? super K> comparator() {
			throw new UnsupportedOperationException();
		}

		@Override public SortedMap<K, V> subMap(final K fromKey, final K toKey) {
			final SortedMap<K, V> result1 = map1.subMap(fromKey, toKey);
			final SortedMap<K, V> result2 = map2.subMap(fromKey, toKey);

			SortedEq(result1, result2);

			SortedEq(map1, map2);

			size();

			return new SortedMapHarness<>(map1, map2, result1, result2);
		}

		@Override public SortedMap<K, V> headMap(final K toKey) {
			final SortedMap<K, V> result1 = map1.headMap(toKey);
			final SortedMap<K, V> result2 = map2.headMap(toKey);

			SortedEq(result1, result2);

			SortedEq(map1, map2);

			size();

			return new SortedMapHarness<>(map1, map2, result1, result2);
		}

		@Override public SortedMap<K, V> tailMap(final K fromKey) {
			final SortedMap<K, V> result1 = map1.tailMap(fromKey);
			final SortedMap<K, V> result2 = map2.tailMap(fromKey);

			SortedEq(result1, result2);

			SortedEq(map1, map2);

			size();

			return new SortedMapHarness<>(map1, map2, result1, result2);
		}

		@Override public K firstKey() {
			final K result1 = map1.firstKey();
			final K result2 = map2.firstKey();

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public K lastKey() {
			final K result1 = map1.lastKey();
			final K result2 = map2.lastKey();

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public int size() {
			final int result1 = map1.size();
			final int result2 = map2.size();

			assert_(result1 == result2);

			SortedEq(map1, map2);

			return result1;
		}

		@Override public boolean isEmpty() {
			throw new UnsupportedOperationException();
		}

		@Override public boolean containsKey(final Object key) {
			final boolean result1 = map1.containsKey(key);
			final boolean result2 = map2.containsKey(key);

			assert_(result1 == result2);

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public boolean containsValue(final Object value) {
			throw new UnsupportedOperationException();
		}

		@Override public V get(final Object key) {
			final V result1 = map1.get(key);
			final V result2 = map2.get(key);

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public V put(final K key, final V value) {
			V result1 = null;
			V result2 = null;

			Exception e1 = null;
			Exception e2 = null;

			try {
				result1 = map1.put(key, value);
			} catch (Exception e) {
				e1 = e;
			}

			try {
				result2 = map2.put(key, value);
			} catch (Exception e) {
				e2 = e;
			}

			assert_(classEq(e1, e2));

			if (e1 != null) {
				return null;
			}

			if (!(result2 instanceof DStructure)) {
				assert_(Objects.equals(result1, result2));
			}

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public V remove(final Object key) {
			final V result1 = map1.remove(key);
			final V result2 = map2.remove(key);

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public void putAll(final Map<? extends K, ? extends V> m) {
			throw new UnsupportedOperationException();
		}

		@Override public void clear() {
			map1.clear();
			map2.clear();

			SortedEq(map1, map2);

			size();
		}

		@Override public @NotNull Set<K> keySet() {
			final Set<K> result1 = map1.keySet();
			final Set<K> result2 = map2.keySet();

			SortedEq(result1, result2);

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public @NotNull Collection<V> values() {
			final Collection<V> result1 = map1.values();
			final Collection<V> result2 = map2.values();

			SortedEq(result1, result2);

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public @NotNull Set<Entry<K, V>> entrySet() {
			final Set<Entry<K, V>> result1 = map1.entrySet();
			final Set<Entry<K, V>> result2 = map2.entrySet();

			return new Set<Entry<K, V>>() {
				@Override
				public int size() {
					final int result12 = result1.size();
					final int result22 = result2.size();

					assert_(result12 == result22);

					SortedEq(result1, result2);

					SortedEq(map1, map2);

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

					SortedEq(result1, result2);

					SortedEq(map1, map2);

					return new Iterator<Entry<K, V>>() {
						@Override
						public boolean hasNext() {
							final boolean result13 = result12.hasNext();
							final boolean result23 = result22.hasNext();

							assert_(result13 == result23);

							SortedEq(result1, result2);

							SortedEq(map1, map2);

							size();

							return result13;
						}

						@Override
						public Entry<K, V> next() {
							final Entry<K, V> result13 = result12.next();
							final Entry<K, V> result23 = result22.next();

							assert_(result13.equals(result23));
							assert_(result23.equals(result13));

							SortedEq(result1, result2);

							SortedEq(map1, map2);

							size();

							return new Entry<K, V>() {
								@Override
								public K getKey() {
									final K result14 = result13.getKey();
									final K result24 = result23.getKey();

									assert_(result14.equals(result24));
									assert_(result24.equals(result14));

									assert_(result13.equals(result23));
									assert_(result23.equals(result13));

									SortedEq(result1, result2);

									SortedEq(map1, map2);

									size();

									return result14;
								}

								@Override
								public V getValue() {
									final V result14 = result13.getValue();
									final V result24 = result23.getValue();

									assert_(result14.equals(result24));
									assert_(result24.equals(result14));

									assert_(result13.equals(result23));
									assert_(result23.equals(result13));

									SortedEq(result1, result2);

									SortedEq(map1, map2);

									size();

									return result14;
								}

								@Override
								public V setValue(final V value) {
									final V result14 = result13.setValue(value);
									final V result24 = result23.setValue(value);

									if (!(result24 instanceof DStructure)) {
										assert_(result14.equals(result24));
										assert_(result24.equals(result14));
									}

									assert_(result13.equals(result23));
									assert_(result23.equals(result13));

									SortedEq(result1, result2);

									SortedEq(map1, map2);

									size();

									return result14;
								}
							};
						}

						@Override
						public void remove() {
							result12.remove();
							result22.remove();

							SortedEq(result1, result2);

							SortedEq(map1, map2);

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

	public static class SortedSetHarness<K, V> implements SortedSet<K> {

		public final NavigableMap<K, V> map1;
		public final NavigableMap<K, V> map2;

		public final SortedSet<K> set1;
		public final SortedSet<K> set2;

		public SortedSetHarness(
				final NavigableMap<K, V> map1, final NavigableMap<K, V> map2,
				final SortedSet<K> set1, final SortedSet<K> set2
		) {
			this.map1 = map1;
			this.map2 = map2;

			this.set1 = set1;
			this.set2 = set2;
		}

		@Override public int size() {
			final int result1 = set1.size();
			final int result2 = set2.size();

			assert_(result1 == result2);

			SortedEq(map1, map2);

			return result1;
		}

		@Override public boolean isEmpty() {
			final boolean result1 = set1.isEmpty();
			final boolean result2 = set2.isEmpty();

			assert_(result1 == result2);

			SortedEq(map1, map2);

			return result1;
		}

		@Override public boolean contains(final Object o) {
			final boolean result1 = set1.contains(o);
			final boolean result2 = set2.contains(o);

			assert_(result1 == result2);

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public @NotNull Iterator<K> iterator() {
			final Iterator<K> result1 = set1.iterator();
			final Iterator<K> result2 = set2.iterator();

			SortedEq(map1, map2);

			return new Iterator<K>() {
				@Override public boolean hasNext() {
					final boolean result12 = result1.hasNext();
					final boolean result22 = result2.hasNext();

					assert_(result12 == result22);

					SortedEq(map1, map2);

					size();

					return result12;
				}

				@Override public K next() {
					final K result12 = result1.next();
					final K result22 = result2.next();

					assert_(Objects.equals(result12, result22));
					assert_(Objects.equals(result22, result12));

					SortedEq(map1, map2);

					size();

					return result12;
				}

				@Override public void remove() {
					result1.remove();
					result2.remove();

					SortedEq(map1, map2);

					size();
				}
			};
		}

		@NotNull @Override public Object[] toArray() {
			final Object[] result1 = set1.toArray();
			final Object[] result2 = set2.toArray();

			assert_(Arrays.equals(result1, result2));

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@NotNull @Override public <T> T[] toArray(final T[] a) {
			final T[] result1 = set1.toArray(a);
			final T[] result2 = set2.toArray(a);

			assert_(Arrays.equals(result1, result2));

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public boolean add(final K k) {
			throw new UnsupportedOperationException();
		}

		@Override public boolean remove(final Object o) {
			final boolean result1 = set1.remove(o);
			final boolean result2 = set2.remove(o);

			assert_(result1 == result2);

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public boolean containsAll(final Collection<?> c) {
			final boolean result1 = set1.containsAll(c);
			final boolean result2 = set2.containsAll(c);

			assert_(result1 == result2);

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public boolean addAll(final Collection<? extends K> c) {
			final boolean result1 = set1.addAll(c);
			final boolean result2 = set2.addAll(c);

			assert_(result1 == result2);

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public boolean retainAll(final Collection<?> c) {
			final boolean result1 = set1.retainAll(c);
			final boolean result2 = set2.retainAll(c);

			assert_(result1 == result2);

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public boolean removeAll(final Collection<?> c) {
			final boolean result1 = set1.removeAll(c);
			final boolean result2 = set2.removeAll(c);

			assert_(result1 == result2);

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public void clear() {
			set1.clear();
			set2.clear();

			SortedEq(map1, map2);

			size();
		}

		@Nullable @Override public Comparator<? super K> comparator() {
			throw new UnsupportedOperationException();
		}

		@Override public @NotNull SortedSet<K> subSet(final K fromElement, final K toElement) {
			final SortedSet<K> result1 = set1.subSet(fromElement, toElement);
			final SortedSet<K> result2 = set2.subSet(fromElement, toElement);

			SortedEq(result1, result2);

			SortedEq(map1, map2);

			size();

			return new SortedSetHarness(map1, map2, result1, result2);
		}

		@Override public @NotNull SortedSet<K> headSet(final K toElement) {
			final SortedSet<K> result1 = set1.headSet(toElement);
			final SortedSet<K> result2 = set2.headSet(toElement);

			SortedEq(result1, result2);

			SortedEq(map1, map2);

			size();

			return new SortedSetHarness(map1, map2, result1, result2);
		}

		@Override public @NotNull SortedSet<K> tailSet(final K fromElement) {
			final SortedSet<K> result1 = set1.tailSet(fromElement);
			final SortedSet<K> result2 = set2.tailSet(fromElement);

			SortedEq(result1, result2);

			SortedEq(map1, map2);

			size();

			return new SortedSetHarness(map1, map2, result1, result2);
		}

		@Override public K first() {
			final K result1 = set1.first();
			final K result2 = set2.first();

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public K last() {
			final K result1 = set1.last();
			final K result2 = set2.last();

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return result1;
		}
	}

	public static class NavigableSetHarness<K, V> implements NavigableSet<K> {

		public final NavigableMap<K, V> map1;
		public final NavigableMap<K, V> map2;

		public final NavigableSet<K> set1;
		public final NavigableSet<K> set2;

		public NavigableSetHarness(
				final NavigableMap<K, V> map1, final NavigableMap<K, V> map2,
				final NavigableSet<K> set1, final NavigableSet<K> set2
		) {
			this.map1 = map1;
			this.map2 = map2;

			this.set1 = set1;
			this.set2 = set2;
		}

		@Override public K lower(final K k) {
			final K result1 = set1.lower(k);
			final K result2 = set2.lower(k);

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public K floor(final K k) {
			final K result1 = set1.floor(k);
			final K result2 = set2.floor(k);

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public K ceiling(final K k) {
			final K result1 = set1.ceiling(k);
			final K result2 = set2.ceiling(k);

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public K higher(final K k) {
			final K result1 = set1.higher(k);
			final K result2 = set2.higher(k);

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public K pollFirst() {
			final K result1 = set1.pollFirst();
			final K result2 = set2.pollFirst();

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public K pollLast() {
			final K result1 = set1.pollLast();
			final K result2 = set2.pollLast();

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public int size() {
			final int result1 = set1.size();
			final int result2 = set2.size();

			assert_(result1 == result2);

			SortedEq(map1, map2);

			return result1;
		}

		@Override public boolean isEmpty() {
			final boolean result1 = set1.isEmpty();
			final boolean result2 = set2.isEmpty();

			assert_(result1 == result2);

			SortedEq(map1, map2);

			return result1;
		}

		@Override public boolean contains(final Object o) {
			final boolean result1 = set1.contains(o);
			final boolean result2 = set2.contains(o);

			assert_(result1 == result2);

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public @NotNull Iterator<K> iterator() {
			final Iterator<K> result1 = set1.iterator();
			final Iterator<K> result2 = set2.iterator();

			SortedEq(map1, map2);

			return new Iterator<K>() {
				@Override public boolean hasNext() {
					final boolean result12 = result1.hasNext();
					final boolean result22 = result2.hasNext();

					assert_(result12 == result22);

					SortedEq(map1, map2);

					size();

					return result12;
				}

				@Override public K next() {
					final K result12 = result1.next();
					final K result22 = result2.next();

					assert_(Objects.equals(result12, result22));
					assert_(Objects.equals(result22, result12));

					SortedEq(map1, map2);

					size();

					return result12;
				}

				@Override public void remove() {
					result1.remove();
					result2.remove();

					SortedEq(map1, map2);

					size();
				}
			};
		}

		@NotNull @Override public Object[] toArray() {
			final Object[] result1 = set1.toArray();
			final Object[] result2 = set2.toArray();

			assert_(Arrays.equals(result1, result2));

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@NotNull @Override public <T> T[] toArray(final T[] a) {
			final T[] result1 = set1.toArray(a);
			final T[] result2 = set2.toArray(a);

			assert_(Arrays.equals(result1, result2));

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public boolean add(final K k) {
			throw new UnsupportedOperationException();
		}

		@Override public boolean remove(final Object o) {
			final boolean result1 = set1.remove(o);
			final boolean result2 = set2.remove(o);

			assert_(result1 == result2);

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public boolean containsAll(final Collection<?> c) {
			final boolean result1 = set1.containsAll(c);
			final boolean result2 = set2.containsAll(c);

			assert_(result1 == result2);

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public boolean addAll(final Collection<? extends K> c) {
			final boolean result1 = set1.addAll(c);
			final boolean result2 = set2.addAll(c);

			assert_(result1 == result2);

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public boolean retainAll(final Collection<?> c) {
			final boolean result1 = set1.retainAll(c);
			final boolean result2 = set2.retainAll(c);

			assert_(result1 == result2);

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public boolean removeAll(final Collection<?> c) {
			final boolean result1 = set1.removeAll(c);
			final boolean result2 = set2.removeAll(c);

			assert_(result1 == result2);

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public void clear() {
			set1.clear();
			set2.clear();

			SortedEq(map1, map2);

			size();
		}

		@NotNull @Override public NavigableSet<K> descendingSet() {
			final NavigableSet<K> result1 = set1.descendingSet();
			final NavigableSet<K> result2 = set2.descendingSet();

			SortedEq(result1, result2);

			SortedEq(map1, map2);

			size();

			return new NavigableSetHarness(map1, map2, result1, result2);
		}

		@NotNull @Override public Iterator<K> descendingIterator() {
			final Iterator<K> result1 = set1.descendingIterator();
			final Iterator<K> result2 = set2.descendingIterator();

			SortedEq(map1, map2);

			return new Iterator<K>() {
				@Override public boolean hasNext() {
					final boolean result12 = result1.hasNext();
					final boolean result22 = result2.hasNext();

					assert_(result12 == result22);

					SortedEq(map1, map2);

					size();

					return result12;
				}

				@Override public K next() {
					final K result12 = result1.next();
					final K result22 = result2.next();

					assert_(Objects.equals(result12, result22));
					assert_(Objects.equals(result22, result12));

					SortedEq(map1, map2);

					size();

					return result12;
				}

				@Override public void remove() {
					result1.remove();
					result2.remove();

					SortedEq(map1, map2);

					size();
				}
			};
		}

		@NotNull @Override
		public NavigableSet<K> subSet(
				final K fromElement, final boolean fromInclusive, final K toElement, final boolean toInclusive
		) {
			final NavigableSet<K> result1 = set1.subSet(fromElement, fromInclusive, toElement, toInclusive);
			final NavigableSet<K> result2 = set2.subSet(fromElement, fromInclusive, toElement, toInclusive);

			SortedEq(result1, result2);

			SortedEq(map1, map2);

			size();

			return new NavigableSetHarness(map1, map2, result1, result2);
		}

		@NotNull @Override public NavigableSet<K> headSet(final K toElement, final boolean inclusive) {
			final NavigableSet<K> result1 = set1.headSet(toElement, inclusive);
			final NavigableSet<K> result2 = set2.headSet(toElement, inclusive);

			SortedEq(result1, result2);

			SortedEq(map1, map2);

			size();

			return new NavigableSetHarness(map1, map2, result1, result2);
		}

		@NotNull @Override public NavigableSet<K> tailSet(final K fromElement, final boolean inclusive) {
			final NavigableSet<K> result1 = set1.tailSet(fromElement, inclusive);
			final NavigableSet<K> result2 = set2.tailSet(fromElement, inclusive);

			SortedEq(result1, result2);

			SortedEq(map1, map2);

			size();

			return new NavigableSetHarness(map1, map2, result1, result2);
		}

		@Nullable @Override public Comparator<? super K> comparator() {
			throw new UnsupportedOperationException();
		}

		@Override public @NotNull SortedSet<K> subSet(final K fromElement, final K toElement) {
			final SortedSet<K> result1 = set1.subSet(fromElement, toElement);
			final SortedSet<K> result2 = set2.subSet(fromElement, toElement);

			SortedEq(result1, result2);

			SortedEq(map1, map2);

			size();

			return new SortedSetHarness(map1, map2, result1, result2);
		}

		@Override public @NotNull SortedSet<K> headSet(final K toElement) {
			final SortedSet<K> result1 = set1.headSet(toElement);
			final SortedSet<K> result2 = set2.headSet(toElement);

			SortedEq(result1, result2);

			SortedEq(map1, map2);

			size();

			return new SortedSetHarness(map1, map2, result1, result2);
		}

		@Override public @NotNull SortedSet<K> tailSet(final K fromElement) {
			final SortedSet<K> result1 = set1.tailSet(fromElement);
			final SortedSet<K> result2 = set2.tailSet(fromElement);

			SortedEq(result1, result2);

			SortedEq(map1, map2);

			size();

			return new SortedSetHarness(map1, map2, result1, result2);
		}

		@Override public K first() {
			final K result1 = set1.first();
			final K result2 = set2.first();

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return result1;
		}

		@Override public K last() {
			final K result1 = set1.last();
			final K result2 = set2.last();

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			size();

			return result1;
		}
	}

	public static class EntryHarness<K, V> implements Entry<K, V> {

		public final NavigableMap<K, V> map1;
		public final NavigableMap<K, V> map2;

		final Entry<K, V> result1;
		final Entry<K, V> result2;

		public EntryHarness(
				final NavigableMap<K, V> map1, final NavigableMap<K, V> map2,
				final Entry<K, V> result1, final Entry<K, V> result2
		) {
			this.map1 = map1;
			this.map2 = map2;

			this.result1 = result1;
			this.result2 = result2;
		}

		@Override public K getKey() {
			final K result12 = result1.getKey();
			final K result22 = result2.getKey();

			assert_(Objects.equals(result12, result22));
			assert_(Objects.equals(result22, result12));

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			return result12;
		}

		@Override public V getValue() {
			final V result12 = result1.getValue();
			final V result22 = result2.getValue();

			assert_(Objects.equals(result12, result22));
			assert_(Objects.equals(result22, result12));

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			return result12;
		}

		@Override public V setValue(final V value) {
			final V result12 = result1.setValue(value);
			final V result22 = result2.setValue(value);

			assert_(Objects.equals(result12, result22));
			assert_(Objects.equals(result22, result12));

			assert_(Objects.equals(result1, result2));
			assert_(Objects.equals(result2, result1));

			SortedEq(map1, map2);

			return result12;
		}
	}
}
