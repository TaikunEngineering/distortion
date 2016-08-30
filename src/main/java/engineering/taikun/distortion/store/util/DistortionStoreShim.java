package engineering.taikun.distortion.store.util;

import engineering.taikun.distortion.ai.api.DistortionAI.Identifier;
import engineering.taikun.distortion.SerializationUtil;
import engineering.taikun.distortion.serialization.api.ByteArray;
import engineering.taikun.distortion.serialization.imp.ArrayWrapper;
import engineering.taikun.distortion.store.api.DistortionStore;
import engineering.taikun.distortion.store.api.DrillingToken;
import engineering.taikun.distortion.store.api.KV;
import engineering.taikun.distortion.store.api.KV.ExpiredReadException;
import engineering.taikun.distortion.store.util.DistortionStoreShim.DistortionStoreShimTransaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Field;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

@SuppressWarnings("unchecked")
public class DistortionStoreShim<BA extends ByteArray<BA>, DT extends DrillingToken<DT>> implements DistortionStore<DistortionStoreShimTransaction> {

	static final ByteArray PREFIX = new ArrayWrapper(new byte[0]);
	static final Object DUMMY_VALUE = new Object();
	static final Future NULL_FUTURE = CompletableFuture.completedFuture(null);
	static final StagedData EMPTY_DATA = new StagedData(Collections.emptyMap(), Collections.emptyMap());

	final long read_token_offset;

	{
		try {
			final Field read_token_field = DistortionStoreShimTransaction.class.getDeclaredField("read_token");
			this.read_token_offset = SerializationUtil.unsafe.objectFieldOffset(read_token_field);
		} catch (final Exception e) {
			throw new RuntimeException("Distortion requires Unsafe to function", e);
		}
	}

	public volatile DT position;
	public DT advancing;

	public final ConcurrentSkipListMap<DistortionStoreShimTransaction, Object> pending = new ConcurrentSkipListMap<>();

	public final SerializationUtil<BA> util;

	final @Nullable HistoryKV<?, BA, DT> ephemeral;
	final @Nullable HistoryKV<?, BA, DT> persistent;

	public DistortionStoreShim(
			final SerializationUtil<BA> util, final DT token,
			final @Nullable Function<DistortionStoreShim, HistoryKV<?, BA, DT>> ephemeral_fn,
			final @Nullable Function<DistortionStoreShim, HistoryKV<?, BA, DT>> persistent_fn
	) {
		if (ephemeral_fn == null && persistent_fn == null)
			throw new IllegalArgumentException("Both stores cannot be null");

		this.util = util;

		this.position = token;
		this.advancing = token.getNextToken();

		this.ephemeral = ephemeral_fn == null ? null : ephemeral_fn.apply(this);
		this.persistent = persistent_fn == null ? null : persistent_fn.apply(this);
	}

	@Override
	public synchronized DistortionStoreShimTransaction newTransaction(
			final Collection<Collection<Identifier>> pending, final Collection<Identifier> identifiers
	) {
		final DT nextToken = this.advancing.getNextToken();

		this.advancing = nextToken;

		final DistortionStoreShimTransaction transaction = new DistortionStoreShimTransaction(
				pending, identifiers, nextToken
		);

		this.pending.put(transaction, DUMMY_VALUE);

		return transaction;
	}

	@Override
	public ArrayList<DistortionStoreShimTransaction> getPendingTransactions() {
		return new ArrayList<>(this.pending.keySet());
	}

	@Override
	public synchronized void shutdown() {
		if (this.ephemeral != null) {
			this.ephemeral.shutdown();
		}

		if (this.persistent != null) {
			this.persistent.shutdown();
		}
	}

	@SuppressWarnings({
			"InstanceVariableMayNotBeInitialized", "unchecked", "ComparableImplementedButEqualsNotOverridden"
			, "ConstantConditions"
	})
	public class DistortionStoreShimTransaction implements DistortionTransaction<DistortionStoreShimTransaction, BA> {

		public final Collection<Identifier> identifiers;
		public final Collection<Collection<Identifier>> captured_identifiers;

		/*
		 * read_token and the soft stuff are only set/read in one thread (Distortion) so there's no need for
		 * synchronization technically, but as a defensive measure (against users who have misbehaving multithreading), we
		 * use unsafe to push the soft stuff so no false null reads
		 */

		@SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
		public DT read_token;
		public final DT transaction_token;
		public @Nullable DT chaining_token;

		@SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
		@Nullable HashMap<ByteArray, BA> ephemeral_soft_state;
		@SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
		@Nullable HashSet<ByteArray> ephemeral_soft_read_set;

		@SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
		@Nullable HashMap<ByteArray, BA> persistent_soft_state;
		@SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
		@Nullable HashSet<ByteArray> persistent_soft_read_set;

		@Nullable final HashSet<ByteArray> ephemeral_read_set;
		@Nullable final HashMap<ByteArray, BA> ephemeral_staging;

		@Nullable final HashSet<ByteArray> persistent_read_set;
		@Nullable final HashMap<ByteArray, BA> persistent_staging;

		int commit_status = 0; // 0 - NOT SET, 1 - PRE-COMMIT, 2 - OK, 3 - FAIL, 4 - EMPTY-CLOSED
		Future<StagedData<BA>> stage_future = null;

		DistortionStoreShimTransaction(
				final Collection<Collection<Identifier>> captured_identifiers,
				final Collection<Identifier> identifiers, final DT transaction_token
		) {
			this.identifiers = identifiers;
			this.captured_identifiers = captured_identifiers;
			this.transaction_token = transaction_token;
			this.chaining_token = null;

			this.ephemeral_read_set = DistortionStoreShim.this.ephemeral == null ? null : new HashSet<>();
			this.ephemeral_staging = DistortionStoreShim.this.ephemeral == null ? null : new HashMap<>();

			this.persistent_read_set = DistortionStoreShim.this.persistent == null ? null : new HashSet<>();
			this.persistent_staging = DistortionStoreShim.this.persistent == null ? null : new HashMap<>();
		}

		@Override
		public Collection<Identifier> getIdentifiers() {
			return this.identifiers;
		}

		@Override
		public Collection<Collection<Identifier>> getPendingIdentifiers() {
			return this.captured_identifiers;
		}

		@Override
		public int compareTo(final @NotNull DistortionStoreShimTransaction that) {
			return this.transaction_token.compareTo(that.transaction_token);
		}

		@Override
		public boolean preceeds(final DistortionStoreShimTransaction that) {
			return this.transaction_token.preceeds(that.transaction_token);
		}

		@Override
		public void start(
				final @Nullable HashMap<ByteArray, BA> ephemeral_soft_state,
				final @Nullable HashMap<ByteArray, BA> persistent_soft_state
		) {
			this.ephemeral_soft_state = ephemeral_soft_state;
			this.persistent_soft_state = persistent_soft_state;

			if (ephemeral_soft_state != null) {
				this.ephemeral_soft_read_set = new HashSet<>();
			}

			if (persistent_soft_state != null) {
				this.persistent_soft_read_set = new HashSet<>();
			}

			// the volatile push also pushes the above

			SerializationUtil.unsafe.putObjectVolatile(
					this, DistortionStoreShim.this.read_token_offset, DistortionStoreShim.this.position
			);
		}

		@Override
		public KV<BA> getEphemeralKV() {
			if (DistortionStoreShim.this.ephemeral == null)
				return null;

			return new KV<BA>() {
				@Override
				@SuppressWarnings("ConstantConditions")
				public @Nullable BA read(final ByteArray key) throws ExpiredReadException {
					final @Nullable BA staging_read = DistortionStoreShimTransaction.this.ephemeral_staging.get(key);

					if (staging_read != null) {
						return staging_read;
					}

					final @Nullable BA soft_read
							= DistortionStoreShimTransaction.this.ephemeral_soft_state == null
							  ? null
							  : DistortionStoreShimTransaction.this.ephemeral_soft_state.get(key);

					if (soft_read != null) {
						DistortionStoreShimTransaction.this.ephemeral_soft_read_set.add(key);
						return soft_read;
					}

					DistortionStoreShimTransaction.this.ephemeral_read_set.add(key);

					return DistortionStoreShim.this.ephemeral.read(key, DistortionStoreShimTransaction.this.read_token);
				}

				@Override
				@SuppressWarnings("ConstantConditions")
				public void write(final ByteArray key, final BA value) {
					DistortionStoreShimTransaction.this.ephemeral_staging.put(key, value);
				}

				@Override
				@SuppressWarnings("ConstantConditions")
				public void delete(final ByteArray key) {
					DistortionStoreShimTransaction.this.ephemeral_staging.put(key, null);
				}

				@Override
				public KV<BA> drill(final ByteArray subkey) {
					return new SubKV<>(DistortionStoreShim.this.util, this, subkey);
				}

				@Override
				public ByteArray getPrefix() {
					return PREFIX;
				}
			};
		}

		@Override
		public KV<BA> getPersistentKV() {
			if (DistortionStoreShim.this.persistent == null)
				return null;

			return new KV<BA>() {
				@Override
				@SuppressWarnings("ConstantConditions")
				public @Nullable BA read(final ByteArray key) throws ExpiredReadException {
					final @Nullable BA staging_read = DistortionStoreShimTransaction.this.persistent_staging.get(key);

					if (staging_read != null) {
						return staging_read;
					}

					final @Nullable BA soft_read
							= DistortionStoreShimTransaction.this.persistent_soft_state == null
							  ? null
							  : DistortionStoreShimTransaction.this.persistent_soft_state.get(key);

					if (soft_read != null) {
						DistortionStoreShimTransaction.this.persistent_soft_read_set.add(key);
						return soft_read;
					}

					DistortionStoreShimTransaction.this.persistent_read_set.add(key);

					return DistortionStoreShim.this.persistent.read(key, DistortionStoreShimTransaction.this.read_token);
				}

				@Override
				@SuppressWarnings("ConstantConditions")
				public void write(final ByteArray key, final BA value) {
					DistortionStoreShimTransaction.this.persistent_staging.put(key, value);
				}

				@Override
				@SuppressWarnings("ConstantConditions")
				public void delete(final ByteArray key) {
					DistortionStoreShimTransaction.this.persistent_staging.put(key, null);
				}

				@Override
				public KV<BA> drill(final ByteArray subkey) {
					return new SubKV<>(DistortionStoreShim.this.util, this, subkey);
				}

				@Override
				public ByteArray getPrefix() {
					return PREFIX;
				}
			};
		}

		@Override
		public synchronized Future<StagedData<BA>> getStagedData() {
			if (this.stage_future == null) {
				// 0 - NOT SET, 1 - PRE-COMMIT, 2 - OK, 3 - FAIL, 4 - EMPTY-CLOSED
				switch (this.commit_status) {
					case 0: {
						this.stage_future = new SettableFuture<>();
						break;
					}
					case 1:
					case 2: {
						this.stage_future
								= CompletableFuture.completedFuture(new StagedData<>(this.ephemeral_staging, this.persistent_staging));
						break;
					}
					case 3: {
						this.stage_future = NULL_FUTURE;
						break;
					}
					case 4: {
						this.stage_future = CompletableFuture.completedFuture(EMPTY_DATA);
						break;
					}
					default:
						throw new IllegalStateException("commit_status was " + this.commit_status);
				}
			}

			return this.stage_future;
		}

		@Override
		public int commit(final boolean block) {

			DistortionStoreShim.this.pending.put(this, Thread.currentThread());

			int spin_count = 0;

			while (true) {
				//noinspection ObjectEquality
				if (this == DistortionStoreShim.this.pending.firstKey()) {
					synchronized (DistortionStoreShim.this) {
						synchronized (this) {
							try {

								final DT position_capture = DistortionStoreShim.this.position;

								if (DistortionStoreShim.this.ephemeral != null) {

									if (this.ephemeral_soft_read_set != null) {
										for (final ByteArray read_index : this.ephemeral_soft_read_set) {
											if (!Objects.equals(
													this.ephemeral_soft_state.get(read_index),
													DistortionStoreShim.this.ephemeral.read(read_index, position_capture)
											)) {
												if (this.commit_status == 0) {
													if (this.stage_future != null) {
														((SettableFuture<StagedData<BA>>) this.stage_future).setValue(null);
													} else {
														this.commit_status = 3;
													}
												} else if (this.commit_status == 1) {
													this.stage_future = NULL_FUTURE;
												}
												return 30; // value changed
											}
										}
									}

									//noinspection ConstantConditions
									for (final ByteArray read_index : this.ephemeral_read_set) {
										if (!Objects.equals(
												DistortionStoreShim.this.ephemeral.read(read_index, this.read_token),
												DistortionStoreShim.this.ephemeral.read(read_index, position_capture)
										)) {
											if (this.commit_status == 0) {
												if (this.stage_future != null) {
													((SettableFuture<StagedData<BA>>) this.stage_future).setValue(null);
												} else {
													this.commit_status = 3;
												}
											} else if (this.commit_status == 1) {
												this.stage_future = NULL_FUTURE;
											}
											return 30; // value changed
										}
									}
								}

								if (DistortionStoreShim.this.persistent != null) {

									if (this.persistent_soft_read_set != null) {
										for (final ByteArray read_index : this.persistent_soft_read_set) {
											if (!Objects.equals(
													this.persistent_soft_state.get(read_index),
													DistortionStoreShim.this.persistent.read(read_index, position_capture)
											)) {
												if (this.commit_status == 0) {
													if (this.stage_future != null) {
														((SettableFuture<StagedData<BA>>) this.stage_future).setValue(null);
													} else {
														this.commit_status = 3;
													}
												} else if (this.commit_status == 1) {
													this.stage_future = NULL_FUTURE;
												}
												return 30; // value changed
											}
										}
									}

									for (final ByteArray read_index : this.persistent_read_set) {
										if (!Objects.equals(
												DistortionStoreShim.this.persistent.read(read_index, this.read_token),
												DistortionStoreShim.this.persistent.read(read_index, position_capture)
										)) {
											if (this.commit_status == 0) {
												if (this.stage_future != null) {
													((SettableFuture<StagedData<BA>>) this.stage_future).setValue(null);
												} else {
													this.commit_status = 3;
												}
											} else if (this.commit_status == 1) {
												this.stage_future = NULL_FUTURE;
											}
											return 30; // value changed
										}
									}
								}

								// passed

								if (DistortionStoreShim.this.ephemeral != null) {
									DistortionStoreShim.this.ephemeral.write(this.ephemeral_staging, this.transaction_token);
								}

								if (DistortionStoreShim.this.persistent != null) {
									DistortionStoreShim.this.persistent.write(this.persistent_staging, this.transaction_token);
								}

								DistortionStoreShim.this.position = this.transaction_token;

								if (this.commit_status == 0) {
									if (this.stage_future != null) {
										((SettableFuture<StagedData<BA>>) this.stage_future).setValue(
												new StagedData<>(this.ephemeral_staging, this.persistent_staging)
										);
									} else {
										this.commit_status = 2;
									}
								}
								return 0;

							} catch (final ExpiredReadException ignored) {
								if (this.commit_status == 0) {
									if (this.stage_future != null) {
										((SettableFuture<StagedData<BA>>) this.stage_future).setValue(null);
									} else {
										this.commit_status = 3;
									}
								} else if (this.commit_status == 1) {
									this.stage_future = NULL_FUTURE;
								}
								return 10; // means couldn't read store again to check if value changed
							}
						}
					}
				} else {
					if (++spin_count > 20) {
						synchronized (this) {
							if (this.commit_status == 0) {
								if (this.stage_future != null) {
									((SettableFuture<StagedData<BA>>) this.stage_future).setValue(
											new StagedData<>(this.ephemeral_staging, this.persistent_staging)
									);
								} else {
									this.commit_status = 1;
								}
							}
						}
						LockSupport.park();
					}
				}
			}
		}

		@Override
		public DistortionStoreShimTransaction chain(
				final Collection<Collection<Identifier>> pending, final Collection<Identifier> identifiers
		) {
			// init chaining token if necessary
			if (this.chaining_token == null) {
				this.chaining_token = this.transaction_token.getDeepToken();
			}

			final DistortionStoreShimTransaction transaction = new DistortionStoreShimTransaction(
					pending, identifiers, this.chaining_token
			);

			// increment chaining token
			this.chaining_token = this.chaining_token.getNextToken();

			DistortionStoreShim.this.pending.put(transaction, DUMMY_VALUE);

			return transaction;
		}

		@Override
		public synchronized void close() {
			if (this.commit_status == 0) {
				this.commit_status = 4;
			}

			if (this.stage_future instanceof SettableFuture) {
				final SettableFuture sf = (SettableFuture) this.stage_future;
				if (sf.value == SettableFuture.NOT_SET) {
					sf.setValue(EMPTY_DATA);
				}
			}

			DistortionStoreShim.this.pending.remove(this);

			final Entry<DistortionStoreShimTransaction, Object> entry = DistortionStoreShim.this.pending.firstEntry();

			if (entry != null && entry.getValue() != DUMMY_VALUE) {
				LockSupport.unpark((Thread) entry.getValue());
			}
		}

	}

	public static class SettableFuture<T> implements Future<T> {

		public static final Object NOT_SET = new Object();

		private Object value = NOT_SET;

		@Override public boolean cancel(final boolean mayInterruptIfRunning) {
			throw new UnsupportedOperationException();
		}

		@Override public boolean isCancelled() {
			throw new UnsupportedOperationException();
		}

		@Override public boolean isDone() {
			throw new UnsupportedOperationException();
		}

		@Override
		public synchronized T get() throws InterruptedException, ExecutionException {
			while (true) {
				if (this.value != NOT_SET)
					return (T) this.value;

				try {
					this.wait();
				} catch (final InterruptedException ignored) {
					// DO NOTHING -- distortion does not use interrupted exceptions, nor do we, ignore it
				}
			}
		}

		@Override
		public synchronized T get(final long timeout, final @NotNull TimeUnit unit)
				throws InterruptedException, ExecutionException, TimeoutException {
			final long kill_time = System.currentTimeMillis() + unit.toMillis(timeout);

			while (true) {
				if (this.value != NOT_SET)
					return (T) this.value;

				if (System.currentTimeMillis() >= kill_time)
					throw new TimeoutException();

				try {
					this.wait(kill_time - System.currentTimeMillis());
				} catch (final InterruptedException ignored) {
					// DO NOTHING -- distortion does not use interrupted exceptions, nor do we, ignore it
				}
			}
		}

		public synchronized void setValue(final @Nullable T value) {
			this.value = value;
			this.notifyAll();
		}
	}

	/**
	 *
	 * @param <BA> ByteArray type of
	 * @param <DT>
	 */
	public interface HistoryKV<IBA extends BA, BA extends ByteArray<BA>, DT extends DrillingToken<DT>> {

		@Nullable IBA read(ByteArray key, DT token) throws ExpiredReadException;

		void write(Map<? extends ByteArray, BA> values, DT token);

		void shutdown();
	}

}
