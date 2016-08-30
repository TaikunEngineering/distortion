package engineering.taikun.distortion.store.imp;

import engineering.taikun.distortion.serialization.api.ByteArray;
import engineering.taikun.distortion.store.api.DrillingToken;
import engineering.taikun.distortion.store.api.KV.ExpiredReadException;
import engineering.taikun.distortion.store.util.DistortionStoreShim;
import engineering.taikun.distortion.store.util.DistortionStoreShim.HistoryKV;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/*
 * General runthrough of how this class works
 *
 * It's a standard ConcurrentHashMap where the values contain a list of values (and DTs)
 *
 * A circular queue is used to reference new entries. Upon dropping an old record, the map entry is cleaned.
 *
 * Values older than the min_age are not guaranteed to be flushed promptly or at all.
 */
public class InMemoryHistoryKV<BA extends ByteArray<BA>, DT extends DrillingToken<DT>> implements
		HistoryKV<BA, BA, DT> {

	public final boolean purge;
	public final long min_age;
	public final long max_size;
	public final long gc_interval;

	private final DistortionStoreShim<BA, DT> parent;
	private final ConcurrentHashMap<ByteArray, MapValue> map;
	private final ArrayDeque<BufferValue> buffer;
	private final Thread gc_thread;

	public InMemoryHistoryKV(final DistortionStoreShim<BA, DT> parent) {
		this(parent, false, 5000, 10_000_000, 5 * 60 * 1000L);
	}

	public InMemoryHistoryKV(final DistortionStoreShim<BA, DT> parent, final boolean purge) {
		this(parent, purge, 5000, 10_000_000, 5 * 60 * 1000L);
	}

	public InMemoryHistoryKV(final DistortionStoreShim<BA, DT> parent, final boolean purge, final long min_age) {
		this(parent, purge, min_age, 10_000_000, 5 * 60 * 1000L);
	}

	public InMemoryHistoryKV(
			final DistortionStoreShim<BA, DT> parent, final boolean purge, final long min_age, final long max_size
	) {
		this(parent, purge, min_age, max_size, 5 * 60 * 1000L);
	}

	public InMemoryHistoryKV(
			final DistortionStoreShim<BA, DT> parent, final boolean purge, final long min_age, final long max_size,
			final long gc_interval
	) {
		this.purge = purge;
		this.min_age = Math.max(0, min_age);
		this.max_size = Math.max(0, max_size);
		this.gc_interval = Math.max(0, gc_interval);

		this.parent = parent;
		this.map = new ConcurrentHashMap<>();
		this.buffer = new ArrayDeque<>(100);
		this.gc_thread = new Thread(() -> {
			while (true) {
				try {
					Thread.sleep(gc_interval);
				} catch (final InterruptedException ignored) {
					return;
				}
				gc();
			}
		}, "InMemoryHistoryKV GC thread");
		this.gc_thread.setDaemon(true);
		this.gc_thread.start();
	}

	@Override
	protected void finalize() throws Throwable {
		this.gc_thread.interrupt();
		super.finalize();
	}

	@Override
	public synchronized void shutdown() {
		this.gc_thread.interrupt();
	}

	@Override
	public @Nullable BA read(final ByteArray key, final DT token) throws ExpiredReadException {
		final @Nullable MapValue map_read = this.map.get(key);

		if (map_read == null) {
			return null;
		}

		synchronized (map_read) {

			// binary search like thing

			// test lowest value, which has to be less or equal than our token

			final int first_compare = map_read.tokens.get(0).compareTo(token);

			if (first_compare > 0) {
				throw new ExpiredReadException();
			}

			if (first_compare == 0 || map_read.tokens.size() == 1) {
				return map_read.values.get(0);
			}

			int low_index = 0;
			int high_index = map_read.tokens.size();

			while (high_index - low_index > 1) {
				final int middle = low_index + (high_index - low_index) / 2;
				final int compare = map_read.tokens.get(middle).compareTo(token);

				if (compare == 0) {
					return map_read.values.get(middle);
				} else if (compare < 0) {
					low_index = middle;
				} else if (compare > 0) {
					high_index = middle;
				}
			}

			return map_read.values.get(low_index);

		}
	}

	@Override
	public synchronized void write(final Map<? extends ByteArray, BA> values, final DT token) {
		final HashSet<ByteArray> buffer_keys = new HashSet<>();

		for (final Entry<? extends ByteArray, BA> entry : values.entrySet()) {
			final ByteArray key = entry.getKey();
			final @Nullable BA value = entry.getValue();

			final @Nullable MapValue map_read = this.map.get(key);

			if (map_read == null) {

				this.map.put(key, new MapValue(
						new ArrayList<>(Collections.singletonList(token)), new ArrayList<>(Collections.singletonList(value))
				));

			} else {

				synchronized (map_read) {

					map_read.tokens.add(token);
					map_read.values.add(value);

				}
			}

			buffer_keys.add(key);
		}

		pushToBuffer(buffer_keys);
	}

	private synchronized void pushToBuffer(final HashSet<ByteArray> keys) {
		final long time = System.currentTimeMillis();
		DT probe = null;

		while (true) {
			final @Nullable BufferValue buffer_value = this.buffer.peek();

			if (buffer_value == null)
				break;

			if (buffer_value.timestamp + this.min_age < time) {
				if (probe == null) {
					probe = this.parent.pending.firstKey().read_token;
				}

				removeBufferHead(probe);
			} else {
				break;
			}
		}

		for (final ByteArray key : keys) {
			if (this.buffer.size() == this.max_size) {
				if (probe == null) {
					probe = this.parent.pending.firstKey().read_token;
				}

				removeBufferHead(probe);
			}

			final BufferValue toinsert = new BufferValue(time, key);

			this.buffer.add(toinsert);
		}
	}

	private synchronized void removeBufferHead(final DT low) {
		final BufferValue buffer_value = this.buffer.remove();

		final @Nullable MapValue map_value = this.map.get(buffer_value.key);

		if (map_value == null)
			return;

		if (
				(this.purge || map_value.values.get(map_value.tokens.size() - 1) == null)
						&& low.compareTo(map_value.tokens.get(map_value.tokens.size() - 1)) > 0
		) {

			this.map.remove(buffer_value.key);

		} else if (map_value.tokens.size() > 1) {

			synchronized (map_value) {

				map_value.tokens.remove(0);
				map_value.values.remove(0);

			}
		}
	}

	public final void gc() {
		if (this.parent.pending.isEmpty())
			return;

		final DT low = this.parent.pending.firstKey().read_token;

		for (final Entry<ByteArray, MapValue> entry : this.map.entrySet()) {
			final MapValue map_value1 = entry.getValue();

			if (
					(this.purge || map_value1.values.get(map_value1.tokens.size() - 1) == null)
							&& low.compareTo(map_value1.tokens.get(map_value1.tokens.size() - 1)) > 0
			) {

				synchronized (this) {

					final @Nullable MapValue map_value2 = this.map.get(entry.getKey());

					if (map_value2 == null)
						continue;

					if (
							(this.purge || map_value2.values.get(map_value2.tokens.size() - 1) == null)
									&& low.compareTo(map_value2.tokens.get(map_value2.tokens.size() - 1)) > 0
					) {

						this.map.remove(entry.getKey());

					}
				}
			}
		}
	}

	private class MapValue {
		final ArrayList<DT> tokens;
		final ArrayList<BA> values;

		MapValue(final ArrayList<DT> tokens, final ArrayList<BA> values) {
			this.tokens = tokens;
			this.values = values;
		}
	}

	private static class BufferValue {
		final long timestamp;
		final ByteArray key;

		BufferValue(final long timestamp, final ByteArray key) {
			this.timestamp = timestamp;
			this.key = key;
		}
	}
}
