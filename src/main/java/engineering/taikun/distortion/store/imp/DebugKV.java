package engineering.taikun.distortion.store.imp;

import engineering.taikun.distortion.serialization.api.ByteArray;
import engineering.taikun.distortion.serialization.imp.ArrayWrapper;
import engineering.taikun.distortion.store.api.KV;
import engineering.taikun.distortion.store.util.SubKV;
import engineering.taikun.distortion.structures.api.DStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.TreeMap;

/**
 * A simple {@link KV} implementation intended for developing/debugging {@link DStructure}s against
 */
public class DebugKV implements KV<ArrayWrapper> {

	public static final ByteArray PREFIX = new ArrayWrapper(new byte[0]);

	public final TreeMap<ByteArray, ArrayWrapper> map = new TreeMap<>();

	@Override
	public @Nullable ArrayWrapper read(final @NotNull ByteArray key) {
		return this.map.get(key);
	}

	@Override
	public void write(final @NotNull ByteArray key, final @NotNull ArrayWrapper value) {
		this.map.put(key, value);
	}

	@Override
	public void delete(final @NotNull ByteArray key) {
		this.map.remove(key);
	}

	@Override
	public KV<ArrayWrapper> drill(@NotNull final ByteArray subkey) {
		return new SubKV<>(ArrayWrapper.UTIL, this, subkey);
	}

	@Override
	public ByteArray getPrefix() {
		return PREFIX;
	}

}
