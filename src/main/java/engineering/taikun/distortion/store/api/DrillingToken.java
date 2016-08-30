package engineering.taikun.distortion.store.api;

/**
 * <p>A token representing a point in a linear stream</p>
 *
 * <p>Positioning is determined solely through simple greater-than, less-than comparisons</p>
 *
 * <p>Drilling in the sense tokens are allowed to generate tokens that are successive in two ways</p>
 *
 * <p>The first way is a simply "advancing". Like going from 1 to 2 to 3.</p>
 *
 * <p>The second way is "drilling". Like going from 1 to 1a to 1a1.</p>
 *
 * <p>Ultimately, these tokens are able to arbitrarily divide the difference between two successive tokens and
 * introduce any number of tokens between these two, previously existing, tokens</p>
 *
 * <p>You can think of a token as representing a line of code being executed. You can advance down the code, and you
 * can also call a method which introduces its own lines of code in between. (This isn't a coincidence)</p>
 *
 * <p>In general, failure should be only be possible in extreme circumstances. The StackToken implementation
 * uses a long for the primary counter, ints for chained counters, and the VM's maximum array length for stack
 * height. Consider this a baseline spec.</p>
 *
 * <p>You can also make stack-overflows impossible, but I think an int's worth of stackframes is a shit load</p>
 *
 * <p>It may also be a good idea to handle a small number of drillings before building a stack since Distortion
 * uses recursion to handle retrying failed commits</p>
 *
 * <p>Each individual token must be stable. If a token's #getNextToken method is called multiple times, it must
 * return a token that is at the same exact place in time-space as all other invocations. (An individual token
 * itself is not responsible for building the tree)</p>
 *
 * @param <T> Your implementation type
 */
public interface DrillingToken<T extends DrillingToken> extends Comparable<T> {

	/**
	 * Gets the next token that's next in the "advancing" sense.
	 *
	 * @return The next token
	 */
	T getNextToken();

	/**
	 * <p>Gets the next "deep" (or drilled) token. This should be greater than this token, but less than the 'next'
	 * token.</p>
	 *
	 * <p>Yes, it must be able to be called after #getNextToken is called</p>
	 *
	 * @return The next deep token
	 */
	T getDeepToken();

	/**
	 * <p>Tests whether this token preceeds the one passed</p>
	 *
	 * <p>Note: The method name is misleading. Specfically, it should be named, "can the passed token be drilled without
	 * violating the simple ordering".</p>
	 *
	 * <pre>
	 *
	 *   A ------------------- H
	 *    \                     \
	 *     B ------- E           I ------- L
	 *      \         \           \         \
	 *       C - D     F - G       J - K     M - N
	 *
	 * </pre>
	 *
	 * <p>Simple examples:</p>
	 *
	 * <p>To compare A to H: A will be compared to I (lesser)</p>
	 *
	 * <p>To compare H to A: H will be compared to B (greater)</p>
	 *
	 * <p>Why you have to go deep:</p>
	 *
	 * <p>To compare A to B: A will be compared to C (lesser)</p>
	 *
	 * <p>To compare B to A: B will be compared to B (equal)</p>
	 *
	 * @param that The token to test against
	 * @return Whether this token preceeds the one passed
	 */
	default boolean preceeds(final T that) {
		return this.compareTo((T) that.getDeepToken()) <= 0;
	}

}
