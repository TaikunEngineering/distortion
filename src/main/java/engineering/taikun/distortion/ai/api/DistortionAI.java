package engineering.taikun.distortion.ai.api;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collection;

/**
 * <p>Supports two methods, train and predict</p>
 *
 * <p>{@link #train} supplies the state, identifier info and the guessed optimal mode</p>
 *
 * <p>{@link #predict} supplies the state and identifier and asks for the predicted mode</p>
 *
 * <p>Specifics</p>
 *
 * <p>Distortion (currently) supports three modes: optimistic, wait-on-identifier&lt;list&lt;identifier&gt;&gt;, and
 * total-stall</p>
 *
 * <p>When a transition is invoked optimistically, its return code indicates which execution mode was guessed to be
 * ideal</p>
 *
 * <table>
 * <tr><td>  0 (success)                </td><td> - optimistic            </td></tr>
 * <tr><td> 10 (dropped)                </td><td> - total-stall           </td></tr>
 * <tr><td> 20 (collision on capture)   </td><td> - wait-on-identifier(s) </td></tr>
 * <tr><td> 30 (collision on later)     </td><td> - wait-on-identifier(s) </td></tr>
 * <tr><td> 40 (unspecified)            </td><td> - wait-on-identifier(s) </td></tr>
 * </table>
 *
 * <p>It is up to the engine to determine which of the three modes is appropriate and which identifier(s) are suspect
 * </p>
 *
 * <p>Abstract class so some of the crap can be dumped here</p>
 */
public interface DistortionAI {

	enum ExecutionMode { OPTIMISTIC, GLOBAL_STALL, FINE_BLOCK }

	/**
	 * <p>Train the AI with the supplied state, identifiers, and determined mode</p>
	 *
	 * <p>Can be run async with multiple invocations simultaneously</p>
	 *
	 * @param state The identifiers of the running transactions when the transaction was started
	 * @param identifiers The identifiers of the transaction in question
	 * @param mode The result code of running this transaction
	 */
	default void train(
			final Collection<Collection<Identifier>> state, final Collection<Identifier> identifiers, final int mode
	) {}

	default Prediction predictHelper(
			final Collection<Collection<Identifier>> state, final Collection<Identifier> identifiers
	) {
		final boolean[] flags = new boolean[state.size()];
		return new Prediction(predict(state, identifiers, flags), flags);
	}

	/**
	 * <p>Make a prediction with the supplied state and identifiers</p>
	 *
	 * <p>You also receive a boolean array. If the determined {@link ExecutionMode} is FINE_BLOCK, setting a cell to
	 * true indicates that the transaction at the corresponding index in state should be blocked on.</p>
	 *
	 * <p>Can be run async with multiple invocations simultaneously</p>
	 *
	 * @param state The identifiers of the currently running transactions
	 * @param identifiers The identifiers of the transaction in question
	 * @param flags A boolean array to set fine_block flags
	 * @return The determined ExecutionMode
	 */
	ExecutionMode predict(
			Collection<Collection<Identifier>> state, Collection<Identifier> identifiers, boolean[] flags
	);

	/**
	 * <p>An array of strings used to identify functions</p>
	 *
	 * <p>For AI purposes, each string is intended to represent the function's place in a hierarchy</p>
	 *
	 * <p>Implements equals, hashCode, and compareTo and is suitable for use in collections. Also has a copy method.</p>
	 */
	class Identifier implements Comparable<Identifier> {
		public final String[] strings;
		public final int hashcode;

		public Identifier(final String... strings) {
			this.strings = strings;
			this.hashcode = Arrays.hashCode(strings);
		}

		private Identifier(final String[] strings, final int hashcode) {
			this.strings = strings;
			this.hashcode = hashcode;
		}

		@Override
		public boolean equals(final Object o) {
			if (this == o)
				return true;

			if (!(o instanceof Identifier))
				return false;

			final Identifier that = (Identifier) o;

			if (this.hashcode != that.hashcode)
				return false;

			return Arrays.equals(this.strings, that.strings);
		}

		@Override
		public int hashCode() {
			return this.hashcode;
		}

		@Override
		public int compareTo(final @NotNull Identifier that) {
			for (int i = 0; i < this.strings.length && i < that.strings.length; i++) {
				final int compare = this.strings[i].compareTo(that.strings[i]);

				if (compare != 0)
					return compare;
			}

			return Integer.compare(this.strings.length, that.strings.length);
		}

		public Identifier copy() {
			return new Identifier(this.strings, this.hashcode);
		}

		@Override
		public String toString() {
			return "Identifier" + Arrays.toString(this.strings);
		}
	}

	class Prediction {
		public final ExecutionMode mode;
		public final boolean[] flags;

		public Prediction(final ExecutionMode mode, final boolean[] flags) {
			this.mode = mode;
			this.flags = flags;
		}
	}

}
