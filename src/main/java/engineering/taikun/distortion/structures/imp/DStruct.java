package engineering.taikun.distortion.structures.imp;

import engineering.taikun.distortion.SerializationUtil;
import engineering.taikun.distortion.serialization.api.ByteArray;
import engineering.taikun.distortion.serialization.imp.ArrayWrapper;
import engineering.taikun.distortion.serialization.imp.ByteByteArray;
import engineering.taikun.distortion.store.api.KV;
import engineering.taikun.distortion.structures.api.DStructure;
import engineering.taikun.distortion.structures.util.AbstractStruct;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("ConstantConditions")
public class DStruct<BA extends ByteArray<BA>> extends AbstractStruct implements DStructure<BA> {

	static final ByteArray SIZE_KEY = new ArrayWrapper(new byte[]{ (byte) -128 });
	static final ByteArray CUSTOM_KEY = new ArrayWrapper(new byte[]{ (byte) -127 });
	static final ByteArray REMOTES_KEY = new ArrayWrapper(new byte[]{ (byte) -126 });

	public KV<BA> kv;
	public ByteArray key;
	public final SerializationUtil<BA> util;
	public final SerializationUtil<BA>.SerializationContext context;

	/**
	 * Non-initializing constructor
	 */
	public DStruct(
			final KV<BA> kv, final ByteArray key, final SerializationUtil<BA> util,
			final SerializationUtil<BA>.SerializationContext context
	) {
		this.kv = kv;
		this.key = key;
		this.util = util;
		this.context = context;
	}

	/**
	 * Initializing constructor, must supply size
	 */
	public DStruct(
			final KV<BA> kv,  final ByteArray key, final SerializationUtil<BA> util,
			final SerializationUtil<BA>.SerializationContext context, final int size
	) {
		this.kv = kv;
		this.key = key;
		this.util = util;
		this.context = context;

		this.kv.write(SIZE_KEY, util.BA(new ByteByteArray((byte) (size - 123))));
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

	@Override
	public synchronized @Nullable String getCustomClass() {
		final BA fetch = this.kv.read(CUSTOM_KEY);

		if (fetch == null) {
			return null;
		}

		return (String) this.context.unpack(null, fetch);
	}

	@Override
	public synchronized void setCustomClass(@NotNull final String className) {
		this.kv.write(CUSTOM_KEY, this.context.serialize(className));
	}

	@Override
	public synchronized ByteArray getRemotesKey() {
		return REMOTES_KEY;
	}

	@Override
	public synchronized void destroy() {
		final int size = this.kv.read(SIZE_KEY).read(0) + 123;

		for (byte i = -123; i < -123 + size; i++) {
			final ByteByteArray index = new ByteByteArray(i);

			final ByteArray read = this.kv.read(index);

			if (read != null) {
				this.context.unpackAndDestroy(this.kv, read);
				this.kv.delete(index);
			}
		}

		this.kv.delete(SIZE_KEY);
		this.kv.delete(CUSTOM_KEY);
		final BA temp; if ((temp = this.kv.read(REMOTES_KEY)) != null) this.context.unpackAndDestroy(this.kv, temp);
		this.kv.delete(REMOTES_KEY);
	}

	@Override
	public synchronized void set(final int index, final Object value) {
		reflectingSet(index, value);
	}

	public synchronized DStructure reflectingSet(final int index, final Object value) {
		if (index > 250) {
			throw new IndexOutOfBoundsException();
		}

		return this.context.store(this.kv, new ByteByteArray((byte) (-123 + index)), value);
	}

	@Override
	public synchronized Object get(final int index) {
		return this.context.unpack(this.kv, this.kv.read(new ByteByteArray((byte) (-123 + index))));
	}

	@Override
	public synchronized int size() {
		return this.kv.read(SIZE_KEY).read(0) + 123;
	}
}
