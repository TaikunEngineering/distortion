package engineering.taikun.distortion.structures.imp;

import engineering.taikun.distortion.serialization.api.ByteArray;
import engineering.taikun.distortion.store.api.KV;
import engineering.taikun.distortion.structures.api.DStructure;
import engineering.taikun.distortion.structures.api.DStructure.DCollectionStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.AbstractSet;
import java.util.Iterator;

public class DSet<T, BA extends ByteArray<BA>> extends AbstractSet<T> implements DCollectionStructure<T, BA> {

	public final DMap<T, Boolean, BA> map;

	public DSet(final DMap<T, Boolean, BA> map) {
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
	public @NotNull Iterator<T> iterator() {
		return this.map.keySet().iterator();
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
