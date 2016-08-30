package engineering.taikun.distortion.structures.imp;

import engineering.taikun.distortion.Distortion;
import engineering.taikun.distortion.SerializationUtil;
import engineering.taikun.distortion.serialization.api.ByteArray;
import engineering.taikun.distortion.serialization.imp.ArrayWrapper;
import engineering.taikun.distortion.store.api.KV;
import engineering.taikun.distortion.structures.api.DStructure;
import engineering.taikun.distortion.structures.api.DStructure.DCollectionStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * <p>An implementation of a random-access {@link List} and a {@link Deque}</p>
 *
 * <p><tt>null</tt> values are allowed</p>
 *
 * <p>This features O(1) reads and O(1) writes at either end</p>
 *
 * <p>This is totally synchronized (thread-safe). Iterating must synchronize on the instance.</p>
 *
 * <p>This operates by using an int-big keyspace and using head and tail 'pointers'</p>
 *
 * <p>Note: While this class is a proper {@link Deque}, the {@link #equals} method only matches {@link List}s</p>
 *
 * @param <T> Element type
 * @param <BA> The type of the {@link ByteArray} implementation
 */
@SuppressWarnings({ "unchecked", "ConstantConditions", "IfStatementWithIdenticalBranches" })
public class DList<T, BA extends ByteArray<BA>> extends AbstractList<T>
		implements DCollectionStructure<T, BA>, Deque<T>, RandomAccess {

	// named 'start' and 'end' which are a little clearer than 'head' and 'tail' imo when dealing with wraparound
	static final ByteArray START_KEY = ArrayWrapper.UTIL.new IntUnion(-2).toBA();
	static final ByteArray END_KEY = ArrayWrapper.UTIL.new IntUnion(-3).toBA();
	static final ByteArray CUSTOM_KEY = ArrayWrapper.UTIL.new IntUnion(-4).toBA();
	static final ByteArray REMOTES_KEY = ArrayWrapper.UTIL.new IntUnion(-5).toBA();

	public final int MAX_HEIGHT;

	public KV<BA> kv;
	public ByteArray key;
	public final SerializationUtil<BA> util;
	public final SerializationUtil<BA>.SerializationContext context;

	public DList(
			final KV<BA> kv, final ByteArray key, final SerializationUtil<BA> util,
			final SerializationUtil<BA>.SerializationContext context, final boolean initialize
	) {
		this.MAX_HEIGHT = Integer.MAX_VALUE;
		this.kv = kv;
		this.key = key;
		this.util = util;
		this.context = context;

		if (initialize) {
			kv.write(START_KEY, util.new IntUnion(0).toBA());
			kv.write(END_KEY, util.new IntUnion(0).toBA());
		}
	}

	/**
	 * <h1>This constructor should only be used when debugging DList itself</h1>
	 */
	public DList(
			final KV<BA> kv, final ByteArray key, final SerializationUtil<BA> util,
			final SerializationUtil<BA>.SerializationContext context, final int MAX_HEIGHT
	) {
		if (!Distortion.DEBUG) {
			throw new IllegalStateException("Not in debug mode");
		}

		this.MAX_HEIGHT = MAX_HEIGHT;
		this.kv = kv;
		this.key = key;
		this.util = util;
		this.context = context;

		kv.write(START_KEY, util.new IntUnion(0).toBA());
		kv.write(END_KEY, util.new IntUnion(0).toBA());
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
	public synchronized void setCustomClass(final @NotNull String className) {
		this.kv.write(CUSTOM_KEY, this.context.serialize(className));
	}

	@Override
	public ByteArray getRemotesKey() {
		return REMOTES_KEY;
	}

	@Override
	public synchronized void destroy() {
		clear();
		this.kv.delete(START_KEY);
		this.kv.delete(END_KEY);
		this.kv.delete(CUSTOM_KEY);
		final BA temp; if ((temp = this.kv.read(REMOTES_KEY)) != null) this.context.unpackAndDestroy(this.kv, temp);
		this.kv.delete(REMOTES_KEY);
	}

	@Override
	public synchronized int size() {
		final int start = this.util.new IntUnion(this.kv.read(START_KEY)).getInt();
		final int end = this.util.new IntUnion(this.kv.read(END_KEY)).getInt();

		//      /- we don't allow end==start because it would be ambiguous if the list was empty or full
		if (end < start) {
			return (this.MAX_HEIGHT - start) + end;
		} else {
			return end - start;
		}
	}

	private static class AddResult {
		final boolean bool_result;
		final DStructure reflection;

		AddResult(final boolean bool_result, final DStructure reflection) {
			this.bool_result = bool_result;
			this.reflection = reflection;
		}
	}

	private AddResult internalAdd(final T t) {

		final int end = this.util.new IntUnion(this.kv.read(END_KEY)).getInt();

		if (end == this.MAX_HEIGHT - 1) {

			if (this.kv.read(this.util.new IntUnion(0).toBA()) != null) {
				throw new IllegalStateException("List capacity exceeded");
			}

			final DStructure reflection = this.context.store(this.kv, this.util.new IntUnion(this.MAX_HEIGHT - 1).toBA(), t);

			this.kv.write(END_KEY, this.util.new IntUnion(this.MAX_HEIGHT).toBA());

			this.modCount++;

			return new AddResult(true, reflection);
		}

		if (end == this.MAX_HEIGHT) {

			final BA one_ba = this.util.new IntUnion(1).toBA();

			if (this.kv.read(one_ba) != null) {
				throw new IllegalStateException("List capacity exceeded");
			}

			final DStructure reflection = this.context.store(this.kv, this.util.new IntUnion(0).toBA(), t);

			this.kv.write(END_KEY, one_ba);

			this.modCount++;

			return new AddResult(true, reflection);
		}

		final BA end_plus_one_ba = this.util.new IntUnion(end + 1).toBA();

		if (this.kv.read(end_plus_one_ba) != null) {
			throw new IllegalStateException("List capacity exceeded");
		}

		final DStructure reflection = this.context.store(this.kv, this.util.new IntUnion(end).toBA(), t);

		this.kv.write(END_KEY, this.util.new IntUnion(end_plus_one_ba).toBA());

		this.modCount++;

		return new AddResult(true, reflection);
	}

	@Override
	public synchronized boolean add(final T t) {
		return internalAdd(t).bool_result;
	}

	@Override
	public synchronized DStructure reflectingAdd(final T t) {
		return internalAdd(t).reflection;
	}

	@Override
	public synchronized boolean addAll(final @NotNull Collection<? extends T> c) {
		if (c.isEmpty()) {
			return false;
		}

		final int c_size = c.size();

		if (c_size > this.MAX_HEIGHT - 1) {
			throw new IllegalStateException("List capacity exceeded");
		}

		final int end = this.util.new IntUnion(this.kv.read(END_KEY)).getInt();
		final long newend_test = end + c_size;

		//              /- MAX is an allowable end point
		if (newend_test > this.MAX_HEIGHT) {

			final int newend = (int) (newend_test - this.MAX_HEIGHT);

			if (this.kv.read(this.util.new IntUnion(newend).toBA()) != null) {
				throw new IllegalStateException("List capacity exceeded");
			}

			int i = end;
			for (final T element : c) {
				if (i == this.MAX_HEIGHT)
					i = 0;

				this.context.store(this.kv, this.util.new IntUnion(i++).toBA(), element);
			}

			this.kv.write(END_KEY, this.util.new IntUnion(newend).toBA());

			this.modCount++;

			return true;
		}

		if (Distortion.DEBUG) {
			if (newend_test == this.MAX_HEIGHT) {
				Thread.currentThread(); // no-op for coverage tracking
			} else {
				Thread.currentThread(); // no-op for coverage tracking
			}
		}

		final int newend = (int) newend_test;

		if (newend == this.MAX_HEIGHT) {
			if (this.kv.read(this.util.new IntUnion(0).toBA()) != null) {
				throw new IllegalStateException("List capacity exceeded");
			}
		} else {
			if (this.kv.read(this.util.new IntUnion(newend).toBA()) != null) {
				throw new IllegalStateException("List capacity exceeded");
			}
		}

		int i = end;
		for (final T element : c) {
			this.context.store(this.kv, this.util.new IntUnion(i++).toBA(), element);
		}

		this.kv.write(END_KEY, this.util.new IntUnion(newend).toBA());

		this.modCount++;

		return true;
	}

	@SuppressWarnings("ReuseOfLocalVariable")
	@Override
	public synchronized boolean addAll(final int index, final Collection<? extends T> c) {
		if (index < 0)
			throw new IndexOutOfBoundsException();

		if (c.isEmpty()) {
			if (index > size()) {
				throw new IndexOutOfBoundsException();
			}
			return false;
		}

		final int c_size = c.size();

		if (c_size > this.MAX_HEIGHT - 1) {
			throw new IllegalStateException("List capacity exceeded");
		}

		if (index == 0) {
			// special case for push

			final int start = this.util.new IntUnion(this.kv.read(START_KEY)).getInt();
			final int newstart_test = start - c_size;

			//                /- 0 is an allowable starting point
			if (newstart_test < 0) {

				final int newstart = this.MAX_HEIGHT - (c_size - start);

				if (this.kv.read(this.util.new IntUnion(newstart - 1).toBA()) != null) {
					throw new IllegalStateException("List capacity exceeded");
				}

				int i = newstart;
				for (final T element : c) {
					if (i == this.MAX_HEIGHT)
						i = 0;

					this.context.store(this.kv, this.util.new IntUnion(i++).toBA(), element);
				}

				this.kv.write(START_KEY, this.util.new IntUnion(newstart).toBA());

				this.modCount++;

				return true;
			}

			if (Distortion.DEBUG) {
				if (newstart_test == 0) {
					Thread.currentThread(); // no-op for coverage tracking
				} else {
					Thread.currentThread(); // no-op for coverage tracking
				}
			}

			//noinspection UnnecessaryLocalVariable
			final int newstart = newstart_test;

			if (newstart == 0) {
				if (this.kv.read(this.util.new IntUnion(this.MAX_HEIGHT - 1).toBA()) != null) {
					throw new IllegalStateException("List capacity exceeded");
				}
			} else {
				if (this.kv.read(this.util.new IntUnion(newstart - 1).toBA()) != null) {
					throw new IllegalStateException("List capacity exceeded");
				}
			}

			int i = newstart;
			for (final T element : c) {
				this.context.store(this.kv, this.util.new IntUnion(i++).toBA(), element);
			}

			this.kv.write(START_KEY, this.util.new IntUnion(newstart).toBA());

			this.modCount++;

			return true;
		}

		final int start = this.util.new IntUnion(this.kv.read(START_KEY)).getInt();
		final int end = this.util.new IntUnion(this.kv.read(END_KEY)).getInt();

		// paid the cost of reading start and end, lower impact than determining/testing new high point
		final int my_size = size();

		if ((long) my_size + c_size > (long) this.MAX_HEIGHT - 1) {
			throw new IllegalStateException("List capacity exceeded");
		}

		if (index > my_size) {
			throw new IndexOutOfBoundsException();
		}

		int i = end - 1;
		Integer newend = null;
		while (true) {
			if ( //                            /- this tests when we 'go past', on the edge, we need to test for 0, not MAX
					i == ((long) start + index - 1 >= this.MAX_HEIGHT
							? (start + index - this.MAX_HEIGHT - 1)
							: (start + index - 1)
					)
			) {
				if (Distortion.DEBUG) {
					if ((long) start + index - 1 == this.MAX_HEIGHT) {
						Thread.currentThread(); // no-op for coverage tracking
					} else {
						Thread.currentThread(); // no-op for coverage tracking
					}
				}

				if (newend == null) {
					// triggered when adding to the end
					//                      /- MAX is an allowable end
					if ((long) end + c_size > (long) this.MAX_HEIGHT) {
						newend = (int) ((long) end + c_size - this.MAX_HEIGHT);
					} else {
						if (Distortion.DEBUG) {
							if ((long) end + c_size == (long) this.MAX_HEIGHT) {
								Thread.currentThread(); // no-op for coverage tracking
							} else {
								Thread.currentThread(); // no-op for coverage tracking
							}
						}

						newend = end + c_size;
					}
				}
				break;
			}

			final BA tempkey = this.util.new IntUnion(i).toBA();
			final T temp = (T) this.context.unpackAndDestroy(this.kv, this.kv.read(tempkey));
			this.kv.delete(tempkey);

			//                    /- can't store at MAX, store at 0
			if ((long) i + c_size >= (long) this.MAX_HEIGHT) {

				if (Distortion.DEBUG) {
					if ((long) i + c_size == (long) this.MAX_HEIGHT) {
						Thread.currentThread(); // no-op for coverage tracking
					} else {
						Thread.currentThread(); // no-op for coverage tracking
					}
				}

				this.context.store(
						this.kv, this.util.new IntUnion((int) ((long) i + c_size - this.MAX_HEIGHT)).toBA(), temp
				);

			} else {

				this.context.store(this.kv, this.util.new IntUnion(i + c_size).toBA(), temp);

			}

			// sets newend upon first iteration
			if (newend == null) {
				//                    /- adds 1, MAX+1 is not an allowable end
				if ((long) i + c_size >= (long) this.MAX_HEIGHT) {
					if (Distortion.DEBUG) {
						if ((long) i + c_size == (long) this.MAX_HEIGHT) {
							Thread.currentThread(); // no-op for coverage tracking
						} else {
							Thread.currentThread(); // no-op for coverage tracking
						}
					}

					newend = (int) ((long) i + c_size - this.MAX_HEIGHT + 1);
				} else {
					newend = i + 1 + c_size;
				}
			}

			if (i == 0) {
				i = this.MAX_HEIGHT - 1;
			} else {
				i--;
			}
		}

		//                       /- can't store at MAX, store at 0
		i = (long) start + index >= (long) this.MAX_HEIGHT
				? start + index - this.MAX_HEIGHT
				: start + index;

		if (Distortion.DEBUG) {
			if ((long) start + index == (long) this.MAX_HEIGHT) {
				Thread.currentThread(); // no-op for coverage tracking
			} else {
				Thread.currentThread(); // no-op for coverage tracking
			}
		}

		for (final T element : c) {

			this.context.store(this.kv, this.util.new IntUnion(i).toBA(), element);

			if (i == this.MAX_HEIGHT - 1) {
				i = 0;
			} else {
				i++;
			}
		}

		this.kv.write(END_KEY, this.util.new IntUnion(newend).toBA());

		this.modCount++;

		return true;
	}

	@Override
	public synchronized T get(final int index) {
		if (index < 0)
			throw new IndexOutOfBoundsException();

		final int start = this.util.new IntUnion(this.kv.read(START_KEY)).getInt();
		//                                     /- nothing to read at MAX
		final BA key_ba = (long) start + index >= (long) this.MAX_HEIGHT
				? this.util.new IntUnion((int) ((long) start + index - this.MAX_HEIGHT)).toBA()
				: this.util.new IntUnion(start + index).toBA();

		if (Distortion.DEBUG) {
			if ((long) start + index == (long) this.MAX_HEIGHT) {
				Thread.currentThread(); // no-op for coverage tracking
			} else {
				Thread.currentThread(); // no-op for coverage tracking
			}
		}

		final BA read_bytes = this.kv.read(key_ba);

		if (read_bytes == null) {
			throw new IndexOutOfBoundsException();
		}

		return (T) this.context.unpack(this.kv, read_bytes);
	}

	@Override
	public synchronized T getLast() {

		final int end = this.util.new IntUnion(this.kv.read(END_KEY)).getInt();
		final BA read_bytes = this.kv.read(this.util.new IntUnion(end - 1).toBA());

		if (read_bytes == null) {
			throw new NoSuchElementException();
		}

		return (T) this.context.unpack(this.kv, read_bytes);
	}

	@Override
	public synchronized T set(final int index, final T element) {
		if (index < 0)
			throw new IndexOutOfBoundsException();

		final int start = this.util.new IntUnion(this.kv.read(START_KEY)).getInt();
		//                                     /- nothing to set at MAX
		final BA key_ba = (long) start + index >= (long) this.MAX_HEIGHT
				? this.util.new IntUnion((int) ((long) start + index - this.MAX_HEIGHT)).toBA()
				: this.util.new IntUnion(start + index).toBA();

		if (Distortion.DEBUG) {
			if ((long) start + index == (long) this.MAX_HEIGHT) {
				Thread.currentThread(); // no-op for coverage tracking
			} else {
				Thread.currentThread(); // no-op for coverage tracking
			}
		}

		final BA read_bytes = this.kv.read(key_ba);

		if (read_bytes == null) {
			throw new IndexOutOfBoundsException();
		}

		final T temp = (T) this.context.unpackAndDestroy(this.kv, read_bytes);

		this.context.store(this.kv, key_ba, element);

		return temp;
	}

	@Override
	public synchronized void add(final int index, final T element) {
		addAll(index, Collections.singleton(element));
	}

	@Override
	public synchronized T remove(final int index) {
		if (index < 0) {
			throw new IndexOutOfBoundsException();
		}

		if (index == 0) {
			// special case

			final int start = this.util.new IntUnion(this.kv.read(START_KEY)).getInt();
			final BA returnkey = this.util.new IntUnion(start).toBA();
			final BA read_bytes = this.kv.read(returnkey);

			if (read_bytes == null) {
				throw new IndexOutOfBoundsException();
			}

			final T toreturn = (T) this.context.unpackAndDestroy(this.kv, read_bytes);
			this.kv.delete(returnkey);

			if (start == this.MAX_HEIGHT - 1) {
				this.kv.write(START_KEY, this.util.new IntUnion(0).toBA());
			} else {
				this.kv.write(START_KEY, this.util.new IntUnion(start + 1).toBA());
			}

			this.modCount++;

			return toreturn;
		}

		final int start = this.util.new IntUnion(this.kv.read(START_KEY)).getInt();
		final int end = this.util.new IntUnion(this.kv.read(END_KEY)).getInt();

		final T toreturn;
		//                       /- nothing to delete at MAX
		if ((long) start + index >= (long) this.MAX_HEIGHT) {

			if (Distortion.DEBUG) {
				if ((long) start + index == (long) this.MAX_HEIGHT) {
					Thread.currentThread(); // no-op for coverage tracking
				} else {
					Thread.currentThread(); // no-op for coverage tracking
				}
			}

			final BA returnkey = this.util.new IntUnion((int) ((long) start + index - this.MAX_HEIGHT)).toBA();
			final BA read_bytes = this.kv.read(returnkey);

			if (read_bytes == null) {
				throw new IndexOutOfBoundsException();
			}

			toreturn = (T) this.context.unpackAndDestroy(this.kv, read_bytes);
			this.kv.delete(returnkey);

			if (end == 1) {

				this.kv.write(END_KEY, this.util.new IntUnion(this.MAX_HEIGHT).toBA());

			} else {

				for (int i = (int) ((long) start + index - this.MAX_HEIGHT); i < end - 1; i++) {
					final BA temp_key = this.util.new IntUnion(i + 1).toBA();
					final T temp_value = (T) this.context.unpackAndDestroy(this.kv, this.kv.read(temp_key));
					this.kv.delete(temp_key);
					this.context.store(this.kv, this.util.new IntUnion(i).toBA(), temp_value);
				}

				this.kv.write(END_KEY, this.util.new IntUnion(end - 1).toBA());

			}

		} else {

			final BA returnkey = this.util.new IntUnion(start + index).toBA();
			final BA read_bytes = this.kv.read(returnkey);

			if (read_bytes == null) {
				throw new IndexOutOfBoundsException();
			}

			toreturn = (T) this.context.unpackAndDestroy(this.kv, read_bytes);
			this.kv.delete(returnkey);

			if (end < start) {

				for (int i = start + index; i < this.MAX_HEIGHT - 1; i++) {
					final BA temp_key = this.util.new IntUnion(i + 1).toBA();
					final T temp_value = (T) this.context.unpackAndDestroy(this.kv, this.kv.read(temp_key));
					this.kv.delete(temp_key);
					this.context.store(this.kv, this.util.new IntUnion(i).toBA(), temp_value);
				}

				{
					final BA temp_key = this.util.new IntUnion(0).toBA();
					final T temp_value = (T) this.context.unpackAndDestroy(this.kv, this.kv.read(temp_key));
					this.kv.delete(temp_key);
					this.context.store(this.kv, this.util.new IntUnion(this.MAX_HEIGHT - 1).toBA(), temp_value);
				}

				for (int i = 0; i < end - 1; i++) {
					final BA temp_key = this.util.new IntUnion(i + 1).toBA();
					final T temp_value = (T) this.context.unpackAndDestroy(this.kv, this.kv.read(temp_key));
					this.kv.delete(temp_key);
					this.context.store(this.kv, this.util.new IntUnion(i).toBA(), temp_value);
				}

				if (end == 1) {
					this.kv.write(END_KEY, this.util.new IntUnion(this.MAX_HEIGHT).toBA());
				} else {
					this.kv.write(END_KEY, this.util.new IntUnion(end - 1).toBA());
				}

			} else {

				for (int i = start + index; i < end - 1; i++) {
					final BA temp_key = this.util.new IntUnion(i + 1).toBA();
					final T temp_value = (T) this.context.unpackAndDestroy(this.kv, this.kv.read(temp_key));
					this.kv.delete(temp_key);
					this.context.store(this.kv, this.util.new IntUnion(i).toBA(), temp_value);
				}

				this.kv.write(END_KEY, this.util.new IntUnion(end - 1).toBA());

			}

		}

		this.modCount++;

		return toreturn;
	}

	@Override
	public synchronized T removeLast() {

		final int end = this.util.new IntUnion(this.kv.read(END_KEY)).getInt();
		final BA returnkey = this.util.new IntUnion(end - 1).toBA();
		final BA read_bytes = this.kv.read(returnkey);

		if (read_bytes == null) {
			throw new NoSuchElementException();
		}

		final T toreturn = (T) this.context.unpackAndDestroy(this.kv, read_bytes);
		this.kv.delete(returnkey);

		if (end == 1) {
			this.kv.write(END_KEY, this.util.new IntUnion(this.MAX_HEIGHT).toBA());
		} else {
			this.kv.write(END_KEY, this.util.new IntUnion(end - 1).toBA());
		}

		return toreturn;
	}

	@Override
	public synchronized @NotNull List<T> subList(final int fromIndex, final int toIndex) {
		return super.subList(fromIndex, toIndex);
	}

	@Override public synchronized boolean isEmpty() {
		return super.isEmpty();
	}

	@Override public synchronized boolean contains(final Object o) {
		return super.contains(o);
	}

	@Override public synchronized @NotNull Iterator<T> iterator() {
		return super.iterator();
	}

	@Override public synchronized void forEach(final Consumer<? super T> action) {
		super.forEach(action);
	}

	@Override public synchronized @NotNull Object[] toArray() {
		return super.toArray();
	}

	@Override public synchronized @NotNull <T1> T1[] toArray(@NotNull final T1[] a) {
		//noinspection SuspiciousToArrayCall
		return super.toArray(a);
	}

	@Override public synchronized boolean remove(final Object o) {
		return super.remove(o);
	}

	@Override public synchronized boolean containsAll(@NotNull final Collection<?> c) {
		return super.containsAll(c);
	}

	@Override public synchronized boolean removeAll(@NotNull final Collection<?> c) {
		return super.removeAll(c);
	}

	@Override public synchronized boolean removeIf(final Predicate<? super T> filter) {
		return super.removeIf(filter);
	}

	@Override public synchronized boolean retainAll(@NotNull final Collection<?> c) {
		return super.retainAll(c);
	}

	@Override public synchronized void replaceAll(final UnaryOperator<T> operator) {
		super.replaceAll(operator);
	}

	@Override public synchronized void sort(final Comparator<? super T> c) {
		super.sort(c);
	}

	@Override public synchronized void clear() {
		super.clear();
	}

	@Override public synchronized int indexOf(final Object o) {
		return super.indexOf(o);
	}

	@Override public synchronized int lastIndexOf(final Object o) {
		return super.lastIndexOf(o);
	}

	@Override public synchronized @NotNull ListIterator<T> listIterator() {
		return super.listIterator();
	}

	@Override public synchronized @NotNull ListIterator<T> listIterator(final int index) {
		return super.listIterator(index);
	}

	/*
	 * All the other shit for Deque<T>
	 */

	@Override public synchronized @NotNull Iterator<T> descendingIterator() {
		return new DescendingIterator();
	}

	@Override public synchronized void addFirst(final T t) {
		add(0, t);
	}

	@Override public synchronized void addLast(final T t) {
		add(t);
	}

	@Override public synchronized boolean offerFirst(final T t) {
		try {

			add(0, t);
			return true;

		} catch (final IllegalStateException ignored) {
			return false;
		}
	}

	@Override public synchronized boolean offerLast(final T t) {
		try {

			add(t);
			return true;

		} catch (final IllegalStateException ignored) {
			return false;
		}
	}

	@Override public synchronized T removeFirst() {
		try {

			return remove(0);

		} catch (final IndexOutOfBoundsException ignored) {
			throw new NoSuchElementException();
		}
	}

	@Override public synchronized @Nullable T pollFirst() {
		try {

			return remove(0);

		} catch (final IndexOutOfBoundsException ignored) {
			return null;
		}
	}

	@Override public synchronized @Nullable T pollLast() {
		try {

			return removeLast();

		} catch (final NoSuchElementException ignored) {
			return null;
		}
	}

	@Override public synchronized T getFirst() {
		try {

			return get(0);

		} catch (final IndexOutOfBoundsException ignored) {
			throw new NoSuchElementException();
		}
	}

	@Override public synchronized @Nullable T peekFirst() {
		try {

			return get(0);

		} catch (final IndexOutOfBoundsException ignored) {
			return null;
		}
	}

	@Override public synchronized @Nullable T peekLast() {
		try {

			return getLast();

		} catch (final NoSuchElementException ignored) {
			return null;
		}
	}

	@Override public synchronized boolean removeFirstOccurrence(final Object o) {
		for (final Iterator<T> it = iterator(); it.hasNext(); ) {
			if (Objects.equals(o, it.next())) {
				it.remove();
				return true;
			}
		}

		return false;
	}

	@Override public synchronized boolean removeLastOccurrence(final Object o) {
		for (final Iterator<T> it = descendingIterator(); it.hasNext(); ) {
			if (Objects.equals(o, it.next())) {
				it.remove();
				return true;
			}
		}

		return false;
	}

	@Override public synchronized boolean offer(final T t) {
		return offerLast(t);
	}

	@Override public synchronized T remove() {
		return removeFirst();
	}

	@Override public synchronized @Nullable T poll() {
		return pollFirst();
	}

	@Override public synchronized T element() {
		return getFirst();
	}

	@Override public synchronized @Nullable T peek() {
		return peekFirst();
	}

	@Override public synchronized void push(final T t) {
		addFirst(t);
	}

	@Override public synchronized T pop() {
		return removeFirst();
	}

	class DescendingIterator implements Iterator<T> {
		private final ListIterator<T> it = listIterator(size());

		@Override
		public boolean hasNext() {
			return this.it.hasPrevious();
		}

		@Override
		public T next() {
			return this.it.previous();
		}

		@Override
		public void remove() {
			this.it.remove();
		}
	}

}
