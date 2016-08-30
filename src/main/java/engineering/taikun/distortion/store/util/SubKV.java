package engineering.taikun.distortion.store.util;

import engineering.taikun.distortion.SerializationUtil;
import engineering.taikun.distortion.serialization.api.ByteArray;
import engineering.taikun.distortion.serialization.util.Glomp;
import engineering.taikun.distortion.store.api.KV;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An implementation of a {@link KV} like one that would be returned by {@link KV#drill}
 *
 * @param <T> The {@link ByteArray} implementation
 */
public class SubKV<T extends ByteArray<T>> implements KV<T> {

	public final SerializationUtil<T> util;
	public final KV<T> source;
	public final ByteArray prefix;

	public SubKV(final SerializationUtil<T> util, final KV<T> source, final ByteArray prefix) {
		this.util = util;
		this.source = source;
		this.prefix = prefix;
	}

	@Override
	public @Nullable T read(@NotNull final ByteArray key) throws ExpiredReadException {
		return this.source.read(new Glomp(this.util, this.prefix, key));
	}

	@Override
	public void write(@NotNull final ByteArray key, @NotNull final T value) {
		this.source.write(new Glomp(this.util, this.prefix, key), value);
	}

	@Override
	public void delete(@NotNull final ByteArray key) {
		this.source.delete(new Glomp(this.util, this.prefix, key));
	}

	@Override
	public KV<T> drill(@NotNull final ByteArray subkey) {
		return new SubKV<>(this.util, this.source, this.util.compose(this.prefix, subkey));
	}

	@Override
	public ByteArray getPrefix() {
		return this.prefix;
	}
}
