package engineering.taikun.distortion.structures.imp;

import engineering.taikun.distortion.serialization.api.ByteArray;
import engineering.taikun.distortion.store.api.KV;
import engineering.taikun.distortion.structures.api.DStructure;
import engineering.taikun.distortion.structures.api.DStructure.DCollectionStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.Map.Entry;

public class DNavigableSet<T, BA extends ByteArray<BA>>
		extends AbstractSet<T> implements DCollectionStructure<T, BA>, NavigableSet<T> {

	public final DNavigableMap<T, Boolean, BA> map;

	public DNavigableSet(final DNavigableMap<T, Boolean, BA> map) {
		this.map = map;
	}

	public short getConcurrencyLevel() {
		return this.map.getConcurrencyLevel();
	}

	@Override
	public KV<BA> getKV() {
		return this.map.getKV();
	}

	@Override
	public void setKV(final KV<BA> kv) {
		this.map.setKV(kv);
	}

	@Override
	public ByteArray getKey() {
		return this.map.getKey();
	}

	@Override
	public void setKey(final ByteArray key) {
		this.map.setKey(key);
	}

	@Override
	public @Nullable String getCustomClass() {
		return this.map.getCustomClass();
	}

	@Override
	public void setCustomClass(final @NotNull String className) {
		this.map.setCustomClass(className);
	}

	@Override
	public ByteArray getRemotesKey() {
		return this.map.getRemotesKey();
	}

	@Override
	public void destroy() {
		this.map.destroy();
	}

	@Override
	public int size() {
		return this.map.size();
	}

	@Override
	public boolean isEmpty() {
		return this.map.isEmpty();
	}

	@SuppressWarnings("SuspiciousMethodCalls")
	@Override
	public boolean contains(final Object o) {
		return this.map.containsKey(o);
	}

	@Override
	public T lower(final T t) {
		return this.map.lowerKey(t);
	}

	@Override
	public T floor(final T t) {
		return this.map.floorKey(t);
	}

	@Override
	public T ceiling(final T t) {
		return this.map.ceilingKey(t);
	}

	@Override
	public T higher(final T t) {
		return this.map.higherKey(t);
	}

	@Override
	public @Nullable T pollFirst() {
		final @Nullable Entry<T, Boolean> entry = this.map.pollFirstEntry();

		if (entry == null) {
			return null;
		}

		return entry.getKey();
	}

	@Override
	public @Nullable T pollLast() {
		final @Nullable Entry<T, Boolean> entry = this.map.pollLastEntry();

		if (entry == null) {
			return null;
		}

		return entry.getKey();
	}

	@Override
	public @NotNull Iterator<T> iterator() {
		return this.map.keySet().iterator();
	}

	@Override
	public @NotNull NavigableSet<T> descendingSet() {
		return this.map.descendingKeySet();
	}

	@Override
	public @NotNull Iterator<T> descendingIterator() {
		return this.map.descendingKeySet().iterator();
	}

	@Override
	public @NotNull NavigableSet<T> subSet(
			final T fromElement, final boolean fromInclusive, final T toElement, final boolean toInclusive
	) {
		return this.map.subMap(fromElement, fromInclusive, toElement, toInclusive).navigableKeySet();
	}

	@Override
	public @NotNull NavigableSet<T> headSet(final T toElement, final boolean inclusive) {
		return this.map.headMap(toElement, inclusive).navigableKeySet();
	}

	@Override
	public @NotNull NavigableSet<T> tailSet(final T fromElement, final boolean inclusive) {
		return this.map.tailMap(fromElement, inclusive).navigableKeySet();
	}

	@Override
	public @Nullable Comparator<? super T> comparator() {
		return this.map.comparator();
	}

	@Override
	public @NotNull NavigableSet<T> subSet(final T fromElement, final T toElement) {
		return this.map.subMap(fromElement, toElement).navigableKeySet();
	}

	@Override
	public @NotNull NavigableSet<T> headSet(final T toElement) {
		return this.map.headMap(toElement).navigableKeySet();
	}

	@Override
	public @NotNull NavigableSet<T> tailSet(final T fromElement) {
		return this.map.tailMap(fromElement).navigableKeySet();
	}

	@Override public T first() {
		return this.map.firstKey();
	}

	@Override public T last() {
		return this.map.lastKey();
	}

	@Override
	public boolean add(final T t) {
		return this.map.put(t, true) == null;
	}

	@Override
	public DStructure reflectingAdd(final T t) {
		return this.map.reflectingPut(t, true).key_reflection;
	}

	@Override
	public boolean remove(final Object o) {
		return this.map.remove(o) != null;
	}

	@Override
	public void clear() {
		this.map.clear();
	}
}
