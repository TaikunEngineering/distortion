package engineering.taikun.distortion;

import engineering.taikun.distortion.ai.api.DistortionAI;
import engineering.taikun.distortion.ai.api.DistortionAI.ExecutionMode;
import engineering.taikun.distortion.ai.api.DistortionAI.Identifier;
import engineering.taikun.distortion.ai.api.DistortionAI.Prediction;
import engineering.taikun.distortion.api.fn.DistortionFunction;
import engineering.taikun.distortion.api.fn.DistortionFunction.ExecutionPolicy;
import engineering.taikun.distortion.SerializationUtil.SerializationContext;
import engineering.taikun.distortion.serialization.api.ByteArray;
import engineering.taikun.distortion.store.api.DistortionStore;
import engineering.taikun.distortion.store.api.DistortionStore.DistortionTransaction;
import engineering.taikun.distortion.store.api.DistortionStore.StagedData;
import engineering.taikun.distortion.store.api.KV;
import engineering.taikun.distortion.store.api.KV.ExpiredReadException;
import engineering.taikun.distortion.structures.imp.DMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.ForkJoinPool.ManagedBlocker;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/*

A preface about this code.

It's complicated. It's a solid block of logic, but much of it is duplicated code.

In essence, our goal is to run everything SUPER PARALLEL. We execute everything the same way; as fast as possible.
We create new, empty transactions and COMMIT() them to serve as our barriers.

We also have children. Children shouldn't commit after an adult fails, which is possible. WE, as in THIS, do the
work to make sure this doesn't happen. We use a MutableInt (un-init/pass/fail) and its monitor as a commit check.

 */

/**
 * <p>The Distortion engine</p>
 *
 * <p>Note: Multiple instances can be run simultaneously, but must not share store</p>
 *
 * @param <T> The transaction type
 * @param <BA> The ByteArray type
 */
@SuppressWarnings({
		"unchecked", "SynchronizationOnLocalVariableOrMethodParameter", "ThrowCaughtLocally",
		"AnonymousInnerClassMayBeStatic"
})
public class Distortion<T extends DistortionTransaction<T, BA>, BA extends ByteArray<BA>> {

	public static final boolean DEBUG = true;

	final int parallelism;

	final DistortionStore<T> store;
	final ForkJoinPool pool;
	final DistortionAI ai;
	final SerializationUtil<BA> util;

	volatile Throwable death_cause = null;

	final ArrayBlockingQueue<QueueItem> queue = new ArrayBlockingQueue<>(16_384);
	final AtomicInteger active_count = new AtomicInteger(0);
	final Thread prodder;
	volatile boolean prodder_live = true;

	final LongAdder jobs_submitted = new LongAdder();
	final LongAdder job_transactions = new LongAdder();
	final LongAdder stall_transactions = new LongAdder();

	public static final Collection<Identifier> GLOBAL_STALL_IDENTIFIERS
			= Collections.singleton(new Identifier("distortion", "global_stall"));

	/**
	 * <p>Create a new Distortion engine</p>
	 *
	 * <p>Note: Root maps have a concurrency level of 1</p>
	 *
	 * @param store DistortionStore for data storage
	 * @param ai DistortionAI for workload behavior prediction
	 * @param util A serialization Util (can be shared between multiple Distortion instances)
	 */
	public Distortion(
			final DistortionStore<T> store, final DistortionAI ai, final SerializationUtil<BA> util, final int parallelism
	) {
		this.store = store;
		this.ai = ai;
		this.util = util;

		this.parallelism = parallelism;

		this.pool = new ForkJoinPool(
				parallelism,
				FJWT::new,
				(t, e) -> e.printStackTrace(),
				false
		);

		try {
			this.pool.submit(() -> {
				final DistortionTransaction<?, BA> t = store.newTransaction(null, null);
				t.start(null, null);

				final KV<BA> ephemeral_KV = t.getEphemeralKV();

				if (ephemeral_KV != null && !DMap.isTouched(ephemeral_KV)) {
					final SerializationContext context = util.new SerializationContext(ephemeral_KV);
					//noinspection ResultOfObjectAllocationIgnored
					new DMap<>(ephemeral_KV, SerializationUtil.EMPTY_ARRAY, util, context, (short) 1);
					context.close();
				}

				final KV<BA> persistent_KV = t.getPersistentKV();

				if (persistent_KV != null && !DMap.isTouched(persistent_KV)) {
					final SerializationContext context = util.new SerializationContext(persistent_KV);
					//noinspection ResultOfObjectAllocationIgnored
					new DMap<>(persistent_KV, SerializationUtil.EMPTY_ARRAY, util, context, (short) 1);
					context.close();
				}

				// this is supposed to be the first and only operation, so it can't fail, in theory
				t.commit(true);
				t.close();
			}).get();
		} catch (InterruptedException | ExecutionException e) {
			throw new RuntimeException(e);
		}

		this.prodder = new Thread(() -> {
			while (this.prodder_live) {

				final boolean doProd;

				if (this.queue.size() < 16_000) {
					Thread.interrupted();
					LockSupport.parkNanos(16_000_000);
					doProd = true;
				} else {
					Thread.yield();
					doProd = false;
				}

				while (this.active_count.get() < this.parallelism - 1) {
					final QueueItem item = this.queue.poll();

					if (item == null)
						break;

					item.future.setWrapped(reallyTransform(item.function, doProd));
				}
			}
		}, "Distortion prodder");
		this.prodder.setDaemon(true);
		this.prodder.start();
	}

//	private synchronized void prod() {
//		while (this.active_count.get() < this.parallelism * 2) {
//			final QueueItem item = this.queue.poll();
//
//			if (item == null)
//				return;
//
//			item.future.setWrapped(reallyTransform(item.function));
//		}
//	}

	/**
	 * Atomically transform Distortion's state with the given operation
	 *
	 * @param operation A DistortionFunction that will be executed against the current state
	 * @return A future that follows the function's progress (and all of the function's children), cannot be cancelled
	 */
	@SuppressWarnings("unchecked")
	public Future<?> transform(final DistortionFunction operation) throws InterruptedException {
		final DistortionFuture future = new DistortionFuture();
		this.queue.put(new QueueItem(operation, future));
//		this.prodder.interrupt();
		return future;
	}

	private Future<?> reallyTransform(final DistortionFunction operation, final boolean doProd) {
		this.active_count.incrementAndGet();
		return this.pool.submit(compileTransform(
				operation, this.store.newTransaction(Collections.EMPTY_LIST, Collections.EMPTY_LIST), null, null, true, doProd
		));
	}

	/**
	 * <p>Shutdown the Distortion engine</p>
	 *
	 * <p>WARNING: Distortion can throw an exception if work is still running/scheduled when you shutdown</p>
	 */
	public void shutdown() throws InterruptedException {
		this.death_cause = new Exception("Distortion has been shutdown");

		this.prodder_live = false;

		this.pool.shutdown();

		this.pool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
	}

	/*
	This is method is complicated... and undocumented for the most part

	Statuses are...
	0 - untouched - do not idle with monitor unless owner
	1 - commit SUCCESSFUL - ok to proceed and commit yourself
	2 - commit FAILED - abandon work if child of failed parent

	parentStatus will always be 1 or 2 when a child grabs monitor, no need to test for 0

	siblings will always succeed, so we can ignore them

	A note on status 0, we cheat and read the status without synchronizing to avoid said synchronizing. 0 lets us know we
	can't cheat and have to grab the monitor and wait it out.
	 */

	/**
	 * @param operation The operation to be run
	 * @param trunk Transaction will be branched off this if present
	 * @param sprout Transaction will be bound to this if present
	 * @param parentStatus The optional status to be waited on if this transaction has a parent
	 */
	private Runnable compileTransform(
			final DistortionFunction operation, final @Nullable T trunk, final @Nullable T sprout,
			@Nullable final MutableInt parentStatus, final boolean base, final boolean doProd
	) {
		return () -> {
			if (this.death_cause != null) {
				throw new IllegalStateException("Distortion is in a failure state", this.death_cause);
			}

			MutableInt myStatus = new MutableInt(0);

			this.jobs_submitted.increment();

			try {
				ArrayList<ForkJoinTask> joinList = null;

				synchronized (myStatus) {

					DistortionFunction activeOperation = operation;
					T transaction = null;
					boolean force_stall = false;

				loop:
					while (true) {
						final ExecutionPolicy policy = activeOperation.getExecutionPolicy();
						final ExecutionMode mode;
						final @Nullable boolean[] flags;

						final @Nullable Collection<T> pending;
						final @Nullable ArrayList<Collection<Identifier>> pending_identifiers;

						if (policy == ExecutionPolicy.USE_AI) {
							pending = (Collection<T>) this.store.getPendingTransactions();
							pending_identifiers = new ArrayList<>(pending.size());

							pending.forEach(e -> pending_identifiers.add(e.getIdentifiers()));
						} else {
							pending = null;
							pending_identifiers = null;
						}

						if (force_stall) {

							mode = ExecutionMode.GLOBAL_STALL;
							flags = null;
							force_stall = false;

						} else if (policy != ExecutionPolicy.USE_AI) {

							if (policy == ExecutionPolicy.FORCE_OPTIMISTIC) {
								mode = ExecutionMode.OPTIMISTIC;
							} else {
								mode = ExecutionMode.GLOBAL_STALL;
							}
							flags = null;

						} else {

							final Prediction prediction = this.ai.predictHelper(
									pending_identifiers, activeOperation.getIdentifiers()
							);
							mode = prediction.mode;
							flags = prediction.flags;

						}

						this.job_transactions.increment();

						ArrayList<ForkJoinTask> branches = null;

						final int commit_status;
						switch (mode) {
							default:
//							case FINE_BLOCK:
							case OPTIMISTIC: {
								// STRATEGY
								// var = commit()
								// train(var)

								if (transaction == null) {
									if (trunk != null) {
										transaction = trunk.chain(pending_identifiers, activeOperation.getIdentifiers());
										trunk.close();
									} else {
										transaction = sprout;
									}
								} else {
									final T oldT = transaction;
									transaction = transaction.chain(pending_identifiers, activeOperation.getIdentifiers());
									oldT.close();
								}

								transaction.start(null, null);

								int status = -1000;
								try {
									eval(transaction, activeOperation);

									final Collection<DistortionFunction> branchFunctions = activeOperation.getBranches();

									if (branchFunctions != null) {
										branches = new ArrayList<>(branchFunctions.size());

										ArrayList<Collection<Identifier>> pending2_identifiers = null;

										for (final DistortionFunction branch : branchFunctions) {
											final ExecutionPolicy branch_policy = branch.getExecutionPolicy();

											if (branch_policy == ExecutionPolicy.USE_AI && pending2_identifiers == null) {
												final Collection<T> pending2 = (Collection<T>) this.store.getPendingTransactions();
												pending2_identifiers = new ArrayList<>(pending2.size());

												for (final T pending_t : pending2) {
													pending2_identifiers.add(pending_t.getIdentifiers());
												}
											}

											final ForkJoinTask task = ForkJoinTask.adapt(
													compileTransform(
															branch, null, transaction.chain(pending2_identifiers, branch.getIdentifiers()),
															myStatus, false, false
													)
											);

											task.fork();

											branches.add(task);

											this.jobs_submitted.increment();
										}
									}

									final boolean parentBlockStatus;
									if (parentStatus == null) {
										parentBlockStatus = true;
									} else {
										switch (parentStatus.getValue()) {
											case 1:
												parentBlockStatus = true;
												break;
											case 2:
												parentBlockStatus = false;
												break;
											case 0:
											default: {
												final BooleanBlocker parentBlocker = new BooleanBlocker() {
													@Override public boolean block() {
														synchronized (parentStatus) {
															setValue(parentStatus.getValue() == 1);
														}
														return true;
													}
												};

												ForkJoinPool.managedBlock(parentBlocker);

												parentBlockStatus = parentBlocker.getValue();
											}
										}
									}

									if (!parentBlockStatus) {
										transaction.close();
										return;
									}

									final T transactionCapture = transaction;
									final PositiveIntBlocker commitBlocker = new PositiveIntBlocker() {
										@Override public boolean block() {
											setValue(transactionCapture.commit(true));
											return true;
										}
									};

									ForkJoinPool.managedBlock(commitBlocker);

									status = commitBlocker.getValue();

								} catch (final Throwable t) {
									force_stall = true;
									status = t instanceof ExpiredReadException ? 10 : 40;
								}

								commit_status = status;

								if (policy == ExecutionPolicy.USE_AI) {
									this.ai.train(transaction.getPendingIdentifiers(), transaction.getIdentifiers(), commit_status);
								}

								break;
							}
							//									case FINE_BLOCK:
							case GLOBAL_STALL: {
								// STRATEGY
								// transaction: { commit() }
								// transaction: { transform() }

								if (transaction == null) {
									if (trunk != null) {
										transaction = trunk.chain(pending_identifiers, GLOBAL_STALL_IDENTIFIERS);
										trunk.close();
									} else {
										transaction = sprout;
									}
								} else {
									final T oldT = transaction;
									transaction = transaction.chain(pending_identifiers, GLOBAL_STALL_IDENTIFIERS);
									oldT.close();
								}

								// todo spin

								transaction.start(null, null);
								final T transactionCapture1 = transaction;
								ForkJoinPool.managedBlock(
										new VoidBlocker() {
											@Override public boolean block() {
												transactionCapture1.commit(true);
												markAsComplete();
												return true;
											}
										}
								);

								this.stall_transactions.increment();

								final T oldT = transaction;
								transaction = transaction.chain(pending_identifiers, activeOperation.getIdentifiers());
								oldT.close();

								transaction.start(null, null);

								try {
									eval(transaction, activeOperation);
								} catch (final Throwable t) {
									throw new ExecutionException(t);
								}

								final Collection<DistortionFunction> branchFunctions = activeOperation.getBranches();

								if (branchFunctions != null) {
									branches = new ArrayList<>(branchFunctions.size());

									ArrayList<Collection<Identifier>> pending2_identifiers = null;

									for (final DistortionFunction branch : branchFunctions) {
										final ExecutionPolicy branch_policy = branch.getExecutionPolicy();

										if (branch_policy == ExecutionPolicy.USE_AI && pending2_identifiers == null) {
											final Collection<T> pending2 = (Collection<T>) this.store.getPendingTransactions();
											pending2_identifiers = new ArrayList<>(pending2.size());

											for (final T pending_t : pending2) {
												pending2_identifiers.add(pending_t.getIdentifiers());
											}
										}

										final ForkJoinTask task = ForkJoinTask.adapt(
												compileTransform(
														branch, null, transaction.chain(pending2_identifiers, branch.getIdentifiers()),
														myStatus, false, false
												)
										);

										task.fork();

										branches.add(task);

										this.jobs_submitted.increment();
									}
								}

								final boolean parentBlockStatus;
								if (parentStatus == null) {
									parentBlockStatus = true;
								} else {
									switch (parentStatus.getValue()) {
										case 1:
											parentBlockStatus = true;
											break;
										case 2:
											parentBlockStatus = false;
											break;
										case 0:
										default: {
											final BooleanBlocker parentBlocker = new BooleanBlocker() {
												@Override public boolean block() {
													synchronized (parentStatus) {
														setValue(parentStatus.getValue() == 1);
													}
													return true;
												}
											};

											ForkJoinPool.managedBlock(parentBlocker);

											parentBlockStatus = parentBlocker.getValue();
										}
									}
								}

								if (!parentBlockStatus) {
									transaction.close();
									return;
								}

								if ((commit_status = transaction.commit(true)) != 0) {
									throw new RuntimeException("Globally stalled transaction failed to commit with 0 status code");
								}

								break;
							}
							case FINE_BLOCK: {
								// STRATEGY
								// bad_transaction.getStagedData()

								final HashMap<ByteArray, BA> ephemeral_soft_state = new HashMap<>();
								final HashMap<ByteArray, BA> persistent_soft_state = new HashMap<>();
//								final MutableBoolean commit_optimistically = new MutableBoolean(true);

								int j = 0;
								for (final T pending_transaction : pending) {

									// don't wait for a transaction set to complete after us
									// and in the case of a sprout, don't wait on our parent

									if (transaction != null && pending_transaction.compareTo(transaction) >= 0) {
										continue;
									}

									if (trunk != null && pending_transaction.compareTo(trunk) >= 0) {
										continue;
									}

									if (sprout != null && sprout.preceeds(pending_transaction)) {
										continue;
									}

//									if (commit_optimistically.getValue() && flags[j]) {
									if (flags[j]) {

										final Future<StagedData<BA>> stagedData_future = pending_transaction.getStagedData();

										final ManagedBlocker blocker = new VoidBlocker() {

//											boolean fresh = true;

											@Override
											public boolean block() throws InterruptedException {
												try {

													final @Nullable StagedData<BA> stagedData;

//													if (this.fresh) {
//														stagedData = stagedData_future.get(Distortion.this.stabilization, TimeUnit.MILLISECONDS);
//													} else {
														stagedData = stagedData_future.get();
//													}

//													this.fresh = false;

													if (stagedData == null) {
														//																System.out.println(pending.size());
//																														commit_optimistically.setValue(false);
													} else {
														if (stagedData.ephermal_map != null) {
															ephemeral_soft_state.putAll(stagedData.ephermal_map);
														}

														if (stagedData.persistent_map != null) {
															persistent_soft_state.putAll(stagedData.persistent_map);
														}
													}
												} catch (final ExecutionException ignored) {
//																												commit_optimistically.setValue(false);
												}
//												catch (final TimeoutException ignored) {
//													Distortion.this.stabilization += 1000;
//													System.out.println("timeout");
//													return false;
//												}
												markAsComplete();
												return true;
											}
										};

//										if (shouldFineStall()) {
//											blockInPlace(blocker);
//										}

										ForkJoinPool.managedBlock(blocker);

									}

									j++;
								}

								if (transaction == null) {
									if (trunk != null) {
										transaction = trunk.chain(pending_identifiers, activeOperation.getIdentifiers());
										trunk.close();
									} else {
										transaction = sprout;
									}
								} else {
									final T oldT = transaction;
									transaction = transaction.chain(pending_identifiers, activeOperation.getIdentifiers());
									oldT.close();
								}

								transaction.start(ephemeral_soft_state, persistent_soft_state);

								// if not commit optimistically, do a blocking transaction like in GLOBAL_STALL

//								if (commit_optimistically.getValue()) {
//									transaction.start(ephemeral_soft_state, persistent_soft_state);
//								} else {
//
//									System.out.println("bad");
//
//									transaction.start(null, null);
//									final T transactionCapture1 = transaction;
//									ForkJoinPool.managedBlock(
//											new VoidBlocker() {
//												@Override public boolean block() {
//													transactionCapture1.commit();
//													markAsComplete();
//													return true;
//												}
//											}
//									);
//
//									this.stall_transactions.increment();
//
//									final T oldT = transaction;
//									transaction = transaction.chain(pending_identifiers, activeOperation.getIdentifiers());
//									oldT.close();
//
//									transaction.start(null, null);
//								}

								int status = -1000;
								try {
									eval(transaction, activeOperation);

									final Collection<DistortionFunction> branchFunctions = activeOperation.getBranches();

									if (branchFunctions != null) {
										branches = new ArrayList<>(branchFunctions.size());

										ArrayList<Collection<Identifier>> pending2_identifiers = null;

										for (final DistortionFunction branch : branchFunctions) {
											final ExecutionPolicy branch_policy = branch.getExecutionPolicy();

											if (branch_policy == ExecutionPolicy.USE_AI && pending2_identifiers == null) {
												final Collection<T> pending2 = (Collection<T>) this.store.getPendingTransactions();
												pending2_identifiers = new ArrayList<>(pending2.size());

												for (final T pending_t : pending2) {
													pending2_identifiers.add(pending_t.getIdentifiers());
												}
											}

											final ForkJoinTask task = ForkJoinTask.adapt(
													compileTransform(
															branch, null, transaction.chain(pending2_identifiers, branch.getIdentifiers()),
															myStatus, false, false
													)
											);

											task.fork();

											branches.add(task);

											this.jobs_submitted.increment();
										}
									}

									final boolean parentBlockStatus;
									if (parentStatus == null) {
										parentBlockStatus = true;
									} else {
										switch (parentStatus.getValue()) {
											case 1:
												parentBlockStatus = true;
												break;
											case 2:
												parentBlockStatus = false;
												break;
											case 0:
											default: {
												final BooleanBlocker parentBlocker = new BooleanBlocker() {
													@Override public boolean block() {
														synchronized (parentStatus) {
															setValue(parentStatus.getValue() == 1);
														}
														return true;
													}
												};

												ForkJoinPool.managedBlock(parentBlocker);

												parentBlockStatus = parentBlocker.getValue();
											}
										}
									}

									if (!parentBlockStatus) {
										transaction.close();
										return;
									}

									boolean set = false;

									for (int i = 0; i < 20; i++) {
										final int temp = transaction.commit(false);

										if (temp >= 0) {
											status = temp;
											set = true;
											break;
										}
									}

									if (!set) {
										final T transactionCapture = transaction;
										final PositiveIntBlocker commitBlocker = new PositiveIntBlocker() {
											@Override public boolean block() {
												setValue(transactionCapture.commit(true));
												return true;
											}
										};

										ForkJoinPool.managedBlock(commitBlocker);

										status = commitBlocker.getValue();
									}

								} catch (final Throwable t) {
									force_stall = true;
									status = t instanceof ExpiredReadException ? 10 : 40;
								}

								commit_status = status;

								break;
							}
						}

						switch (commit_status) {
							case 0: {
								// success

								// all operations after this will be a success, just leave it at 1
								myStatus.setValue(1);

								// add children to join list if present
								if (branches != null) {
									if (joinList == null) {
										joinList = new ArrayList<>();
									}

									joinList.addAll(branches);
								}

								final DistortionFunction protectedFunction = activeOperation.getProtectedFunction();

								if (protectedFunction == null) {
									break loop;
								}

								this.jobs_submitted.increment();

								activeOperation = protectedFunction;
								break;
							}
							case 10:
							case 20:
							case 30:
							case 40: {
								// failure

								// children could have a reference to this status, so create a new one when we try again
								myStatus.setValue(2);
								myStatus = new MutableInt(0);

								if (branches != null) {
									// kill children if possible
									branches.forEach(ForkJoinTask::tryUnfork);
								}

								activeOperation.reset();

								break;
							}
							default:
								throw new RuntimeException("Unknown commit_status: " + commit_status);
						}

					}

					transaction.close();
				}

				if (joinList != null) {
					joinList.forEach(ForkJoinTask::join);
				}

			} catch (final Throwable t) {
				if (t instanceof ExecutionException) {
					this.death_cause = t.getCause();
				} else {
					this.death_cause = new Exception("Error in Distortion transform loop", t);
				}
			}

			if (base) {
				this.active_count.decrementAndGet();

				if (doProd) {
					this.prodder.interrupt();
				}
			}
		};
	}

	private void eval(final T transaction, final DistortionFunction function) {
		SerializationContext ephemeral_context = null;
		SerializationContext persistent_context = null;

		try {
			final KV<BA> ephemeral_KV = transaction.getEphemeralKV();
			final Map ephemeral_map = ephemeral_KV == null
					? null
					: new DMap<>(
							ephemeral_KV, SerializationUtil.EMPTY_ARRAY, this.util,
							ephemeral_context = this.util.new SerializationContext(ephemeral_KV)
					);

			final KV<BA> persistent_KV = transaction.getPersistentKV();
			final Map persistent_map = persistent_KV == null
					? null
					: new DMap<>(
							persistent_KV, SerializationUtil.EMPTY_ARRAY, this.util,
							persistent_context = this.util.new SerializationContext(persistent_KV)
					);

			// the internal ephemeral/persistent ordering is BACKWARDS for the user api
			//noinspection ConstantConditions
			function.transform(persistent_map, ephemeral_map);

		} finally {
			if (ephemeral_context != null)
				ephemeral_context.close();

			if (persistent_context != null)
				persistent_context.close();
		}
	}

	public HashMap<String, Object> getStats() {
		final HashMap<String, Object> toreturn = new HashMap<>();

		toreturn.put("pool size", this.pool.getPoolSize());

		toreturn.put("jobs submitted", this.jobs_submitted.sum());
		toreturn.put("job transactions", this.job_transactions.sum());
		toreturn.put("stall transactions", this.stall_transactions.sum());

		return toreturn;
	}

	@Override
	protected void finalize() throws Throwable {
		this.prodder_live = false;
		super.finalize();
	}

	static class QueueItem {
		public final DistortionFunction function;
		public final DistortionFuture future;

		QueueItem(final DistortionFunction function, final DistortionFuture future) {
			this.function = function;
			this.future = future;
		}
	}

	static class DistortionFuture<T> implements Future<T> {
		private volatile Future<T> wrapped = null;
		private boolean flagged = false;

		DistortionFuture() {}

		DistortionFuture(final Future<T> wrapped) {
			this.wrapped = wrapped;
		}

		@Override public boolean cancel(final boolean mayInterruptIfRunning) {
			return false;
		}

		@Override public boolean isCancelled() {
			return false;
		}

		@Override public boolean isDone() {
			if (this.wrapped == null) {
				return false;
			}

			return this.wrapped.isDone();
		}

		@Override public T get() throws InterruptedException, ExecutionException {
			while (this.wrapped == null) {
				synchronized (this) {
					flagged = true;
					this.wait();
				}
			}

			return this.wrapped.get();
		}

		@Override public T get(final long timeout, final @NotNull TimeUnit unit)
				throws InterruptedException, ExecutionException, TimeoutException {
			final long end_time = System.currentTimeMillis() + unit.toMillis(timeout);

			while (this.wrapped == null) {
				synchronized (this) {
					flagged = true;
					this.wait(Math.max(0, end_time - System.currentTimeMillis()));
				}
			}

			return this.wrapped.get(timeout, unit);
		}

		public synchronized void setWrapped(final Future<T> wrapped) {
			this.wrapped = wrapped;
			if (flagged)
				this.notifyAll();
		}
	}

	static class MutableInt {
		public int value;

		MutableInt(final int value) {
			this.value = value;
		}

		public int getValue() {
			return this.value;
		}

		public void setValue(final int value) {
			this.value = value;
		}
	}

	static class MutableBoolean {
		public boolean value;

		MutableBoolean(final boolean value) {
			this.value = value;
		}

		public boolean getValue() {
			return this.value;
		}

		public void setValue(final boolean value) {
			this.value = value;
		}
	}

	static abstract class VoidBlocker implements ManagedBlocker {
		private volatile boolean isDone = false;

		@Override
		public boolean isReleasable() {
			return this.isDone;
		}

		public void markAsComplete() {
			this.isDone = true;
		}
	}

	static abstract class BooleanBlocker implements ManagedBlocker {
		// 0 - not set; 1 - true; 2 - false
		private volatile int value = 0;

		@Override
		public boolean isReleasable() {
			return this.value != 0;
		}

		public void setValue(final boolean value) {
			if (value) {
				this.value = 1;
			} else {
				this.value = 2;
			}
		}

		public boolean getValue() {
			return this.value == 1;
		}
	}

	static abstract class PositiveIntBlocker implements ManagedBlocker {
		// -1 - not set; 0 -> MAX_INT - value set
		private volatile int value = -1;

		@Override
		public boolean isReleasable() {
			return this.value >= 0;
		}

		public void setValue(final int value) {
			this.value = value;
		}

		public int getValue() {
			return this.value;
		}
	}

	public static class FJWT extends ForkJoinWorkerThread {
		public boolean locked = false;

		protected FJWT(final ForkJoinPool pool) {
			super(pool);
		}

		public static Consumer<Boolean> setThreadLock() {
			return b -> ((FJWT) Thread.currentThread()).locked = b;
		}

		public static BooleanSupplier getTheadLock() {
			return () -> ((FJWT) Thread.currentThread()).locked;
		}
	}

}
