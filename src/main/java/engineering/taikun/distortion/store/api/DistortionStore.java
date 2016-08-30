package engineering.taikun.distortion.store.api;

import engineering.taikun.distortion.ai.api.DistortionAI.Identifier;
import engineering.taikun.distortion.serialization.api.ByteArray;
import engineering.taikun.distortion.store.api.DistortionStore.DistortionTransaction;
import engineering.taikun.distortion.store.api.KV.ExpiredReadException;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * <p>Represents a compatible storage back-end</p>
 *
 * <p>Implementations are responsible for safely, concurrently storing all data, creating transactions, and maintaining
 * the ordering of transaction committing (including blocking)</p>
 *
 * <p>This interface makes heavy use of generics allowing implementations to easily code against other particular
 * implementations if so desired</p>
 *
 * <p>Note: This interface is only meant to be implemented/used by back-end implementers/people familiar with the grit
 * of manual concurrency and transactional memory. Distortion's correctness relies on your store's correctness.
 *
 * <p>Note: You are free to drop {@link InterruptedException}s as part of blocking. Distortion does not use
 * interrupts to control its workers and it maintains its own thread pool.</p>
 *
 * <p>Note: The inner class/recursive generics mess can cause problems compiling and/or your IDE's warnings/inspections
 * to fail. Using JDK 8 or the equivalent Eclipse compiler seems to work most of the time however. Intellij seems to
 * understand it all better than either compiler, so make sure your implementation still compiles when making changes,
 * even if Intellij doesn't flag the change as illegal. I haven't tested for Eclipse IDE support, but I do use the
 * compiler (which does mostly work).</p>
 *
 * <p>The documentation for this is intentionally kept sparse. <i>Everything</i> is important, read it all.</p>
 */
public interface DistortionStore<T extends DistortionTransaction> {

	/**
	 * <p>Creates a new top-level transaction and reserves the slot in time-space. This action dictates the serialization
	 * order (NOT the #start order in the transactions).</p>
	 *
	 * <p>The identifiers of the pending transactions and the identifiers of this transaction are passed in by
	 * Distortion, and may be null. (However, Distortion only requires you later pass the exact values here (or
	 * collections equivalent))</p>
	 *
	 * <p>This operation must be atomic and allow multiple instances running (or be completely synchro)</p>
	 *
	 * <p>Exact ordering is required (when called successively by the same thread) however thread/core biases are
	 * allowed. However, be aware starvation characteristics partly define the quality of your implementation.</p>
	 *
	 * @param pending The pending identifiers that Distortion captured a little earlier
	 * @param identifiers The identifiers for this particular transaction
	 * @return The generated transaction
	*/
	T newTransaction(
			@Nullable Collection<Collection<Identifier>> pending, @Nullable Collection<Identifier> identifiers
	);

	/**
	 * <p>Get all pending transactions <b>in sorted order</b></p>
	 *
	 * <p>Like #newTransaction(), this operation must be atomic and allow multiple instances or be synchro</p>
	 *
	 * @return A collection of the pending transactions, sorted
	 */
	Collection<? extends T> getPendingTransactions();

	/**
	 * Shutdown, should clean up. Implementations should assume no pending transactions.
	 */
	void shutdown();

	/**
	 * <p>The basic transaction work is done with</p>
	 *
	 * <p>All transactions must be started as their first operation</p>
	 *
	 * <p>All transactions must be closed as their last operation (that is, if they don't, permanently hang the
	 * store)</p>
	 *
	 * <p>Reading, writing and committing are optional</p>
	 *
	 * <p>Committing multiple times per transaction is forbidden, however you are not required to enforce this</p>
	 *
	 * <p>Reading or writing without a successive commit should do nothing to the store's state</p>
	 *
	 * <p>Just as DrillingToken's are comparable and can test for descension, so must your transactions</p>
	 *
	 * @param <T> The type of your implementation
	 * @param <BA> The type of the ByteArray (can be left generic or you can specialize and require a certain imp)
	 */
	interface DistortionTransaction<T extends DistortionTransaction, BA extends ByteArray> extends Comparable<T> {

		/**
		 * Return the collection of identifiers passed in as part of construction (or return an object equivalent to it)
		 *
		 * @return This transaction's identifiers
		 */
		@Nullable Collection<Identifier> getIdentifiers();

		/**
		 * Return the collection of collections of identifiers passed in as part of construction (or return an object
		 * equivalent to it)
		 *
		 * @return The identifiers of the pending transactions
		 */
		Collection<Collection<Identifier>> getPendingIdentifiers();

		/**
		 * <p>Tests whether this transaction preceeds from the one passed</p>
		 *
		 * <p>This should call the #preceeds method on your {@link DrillingToken}. Simple comparisons are NOT sufficient
		 * to implement this method.</p>
		 *
		 * @param transaction The transaction to test against
		 * @return Whether this transaction preceeds the one passed
		 */
		boolean preceeds(T transaction);

		/**
		 * <p>Starts the transaction by capturing the read slot and possibly receiving maps of values the
		 * transaction should treat as initial state. (A null value only indicates no data is being passed in,
		 * ephemeral/persistent state must always be tracked as originally declared)</p>
		 *
		 * <p>These values are speculative, and must be checked against the current state at commit time</p>
		 *
		 * <p>You can also ignore these values if you so choose, however failure rates are likely to be higher if you do
		 * so</p>
		 *
		 * <p>While getting the transaction via #newTransaction will create the transaction and reserve its slot in
		 * time-space, the transaction will not capture the read slot in time-space. (Which differ because Distortion is
		 * all about enabling optimistic concurrency and the various hacks that are implied as much)</p>
		 *
		 * <p>Or, as a concrete example, this allows me to delay starting if I know there is currently an interfering
		 * operation and I don't want to redo my work</p>
		 *
		 * @param ephemeral_soft_state state from previous transactions, possibly null, must be verified at commit time
		 * @param persistent_soft_state state from previous transactions, possibly null, must be verified at commit time
		 */
		void start(
				@Nullable HashMap<ByteArray, BA> ephemeral_soft_state, @Nullable HashMap<ByteArray, BA> persistent_soft_state
		);

		/**
		 * <p>Return a {@link KV} whose operations are staged as part of the transaction</p>
		 *
		 * <p>Note: Reads <b>must</b> return the exact value as present of the read-slot capture (or throw a
		 * {@link ExpiredReadException})</p>
		 *
		 * <p>Remember that these methods should only stage their results</p>
		 *
		 * <p>Remember to read from the 'written' values first</p>
		 *
		 * @return A {@link KV} that maps to the ephemeral store or null if not supported
		 */
		@Nullable KV<BA> getEphemeralKV();

		/**
		 * @see #getEphemeralKV
		 *
		 * @return A {@link KV} that maps to the ephemeral store or null if not supported
		 */
		@Nullable KV<BA> getPersistentKV();

		/**
		 * <p>Called by an external thread to get the staged results of the transaction</p>
		 *
		 * <p>Should block until commit is called. However, if the transaction is ready to attempted to be committed, you
		 * can wait until it completes before returning.</p>
		 *
		 * <p>If the transaction has failed to commit, you can indicate this by returning a future that returns null</p>
		 *
		 * <p>The individual maps within the staged data can be null to indicate an empty map (no data to write as part of
		 * the transaction)</p>
		 *
		 * <p>Note: only #get is invoked, all other Future methods don't need to be implemented</p>
		 *
		 * @return a {@link StagedData}
		 */
		Future<StagedData<BA>> getStagedData();

		/**
		 * <p>Attempts (AND BLOCKS if not immediate transaction) to commit the transaction. Returns a status code.</p>
		 *
		 * -10 -> attempted, not able to commit                                             <br />
		 *   0 -> success                                                                   <br />
		 *  10 -> dropped / fell off queue                                                  <br />
		 *  20 -> collision with transaction that was current at read-capture               <br />
		 *  30 -> collision with transaction that created after read-capture                <br />
		 *  40 -> nonspecific failure                                                       <br />
		 *
		 * <p>There are three classes of return codes, negative, zero, and postive. The smaller the return value, the more
		 * preferred it is. However, if your implementation does not distinguish between the errors or some subset or
		 * whatever, don't waste time trying to determine if it could count as a lower one.</p>
		 *
		 * <p>Remember, don't drop the transaction until it's closed. It can be chained.</p>
		 *
		 * <p>You should not spin as part of blocking, Distortion uses the block parameter to spin as it sees fit</p>
		 *
		 * @param block Block until success if true, only attempt if false
		 * @return Status code
		 */
		int commit(boolean block);

		/**
		 * <p>Extends the event this transaction is in by creating a new transaction and inserting it after this event and
		 * all other transactions created by chaining in time-space.</p>
		 *
		 * <p>Must be called before closing the transaction</p>
		 *
		 * <p>Chaining does NOT guarantee that the transactions have the same read point. The chained transaction must
		 * still be started and closed normally.</p>
		 *
		 * <p>multiple calls to #chain should create a tree like so...</p>
		 *
		 * this_transaction              <br />
		 * -> chain_transaction_0        <br />
		 * --> chain_transaction_0_0     <br />
		 * -> chain_transaction_1        <br />
		 *
		 * <p>Hint: This requires running #getNextToken from the last transaction's DrillingToken, so keep track of it</p>
		 *
		 * @param pending The captured pending transaction identifiers
		 * @param identifiers The chained transaction's identifiers
		 * @return A transaction with time position after and deeper than this transaction
		 */
		T chain(
				@Nullable Collection<Collection<Identifier>> pending, @Nullable Collection<Identifier> identifiers
		);

		/**
		 * <p>Closes the transaction, allowing the store to process successive events</p>
		 *
		 * <p>Don't forget, someone could still chain a transaction that's idling after committing</p>
		 */
		void close();
	}

	final class StagedData<BA extends ByteArray> {
		public final @Nullable Map<ByteArray, BA> ephermal_map;
		public final @Nullable Map<ByteArray, BA> persistent_map;

		public StagedData(
				final @Nullable Map<ByteArray, BA> ephermal_map, final @Nullable Map<ByteArray, BA> persistent_map
		) {
			this.ephermal_map = ephermal_map;
			this.persistent_map = persistent_map;
		}
	}
}
