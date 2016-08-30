package engineering.taikun.distortion.api.fn;

import engineering.taikun.distortion.ai.api.DistortionAI.Identifier;
import engineering.taikun.distortion.SerializationUtil;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * <p>The basic building block of transactions</p>
 *
 * <p>If only {@link #getIdentifiers} and {@link #transform} are defined, the function is essentially equivalent to
 * a Runnable.</p>
 *
 * <p>It is possible to do more advanced things by overriding {@link #getBranches} and/or
 * {@link #getProtectedFunction}, however you are encouraged to use {@link AdvancedFunction} unless you're
 * writing your own work assignment system.</p>
 *
 * <p>It is also possible to do simpler things by using {@link OptimisticFunction} or {@link StalledFunction} which
 * require only defining the code to be executed and bypass the AI system</p>
 */
public interface DistortionFunction {

	/**
	 * Get this function's identifiers
	 *
	 * @return The identifiers as a collection of {@link Identifier}s
	 */
	@Nullable Collection<Identifier> getIdentifiers();

	/**
	 * <p>Transform the system state</p>
	 *
	 * <p>Persistent: data persists between runs</p>
	 *
	 * <p>Ephemeral: persists only for the duration of the application</p>
	 *
	 * <p>Note: The maps may be null, however you should document which maps you need and assume your needs are met</p>
	 *
	 * @param persistent The persisted Map
	 * @param ephemeral The ephemeral Map
	 */
	void transform(Map persistent, Map ephemeral);

	// TODO change docs to reflect reinsertion
//
//	/**
//	 * <p>Create a copy of a Distortion synthesized object where the backing structure is heap-allocated (a copy of the
//	 * Distortion state at the time of this being run)</p>
//	 *
//	 * <p>The resulting copy is suitable for passing out of a DistortionFunction as it is not linked to the Distortion
//	 * memory system</p>
//	 *
//	 * <p>Note: The copy will remain writeable, but changes are not propagated to the Distortion state</p>
//	 *
//	 * @param object A {@link DistortionListObject} synthesized by Distortion
//	 * @param <T> The type of your custom object
//	 * @return An independent clone with its own memory
//	 */
//	default <T extends DistortionListObject> T export(final T object) {
//		return SerializationUtil.export(object, object.list());
//	}
//
//	/**
//	 * <p>Create a copy of a Distortion synthesized object where the backing structure is heap-allocated (a copy of the
//	 * Distortion state at the time of this being run)</p>
//	 *
//	 * <p>The resulting copy is suitable for passing out of a DistortionFunction as it is not linked to the Distortion
//	 * memory system</p>
//	 *
//	 * <p>Note: The copy will remain writeable, but changes are not propagated to the Distortion state</p>
//	 *
//	 * @param object A {@link DistortionMapObject} synthesized by Distortion
//	 * @param <T> The type of your custom object
//	 * @return An independent clone with its own memory
//	 */
//	default <T extends DistortionMapObject> T export(final T object) {
//		return SerializationUtil.export(object, object.map());
//	}
//
//	/**
//	 * <p>Create a copy of a Distortion synthesized object where the backing structure is heap-allocated (a copy of the
//	 * Distortion state at the time of this being run)</p>
//	 *
//	 * <p>The resulting copy is suitable for passing out of a DistortionFunction as it is not linked to the Distortion
//	 * memory system</p>
//	 *
//	 * <p>Note: The copy will remain writeable, but changes are not propagated to the Distortion state</p>
//	 *
//	 * @param object A {@link DistortionSetObject} synthesized by Distortion
//	 * @param <T> The type of your custom object
//	 * @return An independent clone with its own memory
//	 */
//	default <T extends DistortionSetObject> T export(final T object) {
//		return SerializationUtil.export(object, object.set());
//	}

	/**
	 * <p>Create a heap-allocated copy of the data structure</p>
	 *
	 * <p>The resulting copy is suitable for passing out of a DistortionFunction as it is not linked to the Distortion
	 * memory system</p>
	 *
	 * <p>Note: The copy will remain writeable, but changes are not propagated to the Distortion state</p>
	 *
	 * @param object A {@link List} returned by Distortion
	 * @return A copy of the structure with its own memory
	 */
	default <T> List<T> export(final List object) {
		return SerializationUtil.export(object);
	}

	/**
	 * <p>Create a heap-allocated copy of the data structure</p>
	 *
	 * <p>The resulting copy is suitable for passing out of a DistortionFunction as it is not linked to the Distortion
	 * memory system</p>
	 *
	 * <p>Note: The copy will remain writeable, but changes are not propagated to the Distortion state</p>
	 *
	 * @param object A {@link Map} returned by Distortion
	 * @return A copy of the structure with its own memory
	 */
	default <K, V> Map<K, V> export(final Map object) {
		return SerializationUtil.export(object);
	}

	/**
	 * <p>Create a heap-allocated copy of the data structure</p>
	 *
	 * <p>The resulting copy is suitable for passing out of a DistortionFunction as it is not linked to the Distortion
	 * memory system</p>
	 *
	 * <p>Note: The copy will remain writeable, but changes are not propagated to the Distortion state</p>
	 *
	 * @param object A {@link Set} returned by Distortion
	 * @return A copy of the structure with its own memory
	 */
	default <T> Set<T> export(final Set object) {
		return SerializationUtil.export(object);
	}

	/**
	 * <p>Returns a Collection of DistortionFunctions that will be executed in the timespace immediately after this
	 * function.</p>
	 *
	 * <p>That is, if this were normal, linear code, these functions would be executed sequentially after this function's
	 * #transform completes.</p>
	 *
	 * @return A nullable collection of the branches, defaults to null
	 */
	default @Nullable Collection<DistortionFunction> getBranches() {
		return null;
	}

	/**
	 * <p>Returns a DistortionFunction that is protected (guaranteed to run exactly once) and that will be executed
	 * in the timespace immediately after this function.</p>
	 *
	 * <p>This function waits for _all_ functions before it to complete before starting.</p>
	 *
	 * <p>Use protected functions to interact with the outside world where repeated, potentially different interactions
	 * are an error condition.</p>
	 *
	 * @return A nullable function that will be executed in a protected environment, defaults to null
	 */
	default @Nullable DistortionFunction getProtectedFunction() {
		return null;
	}

	/**
	 * <p>If the function isn't stateless, reset before being executed again as part of transaction failure</p>
	 *
	 * <p>For example, if #transform can schedule functions to be branched, use this method to clear those functions</p>
	 *
	 * <p>Defaults to doing nothing</p>
	 */
	default void reset() {}

	/**
	 * <p>Functions can override their execution policy, potentially bypassing the scheduling AI</p>
	 *
	 * <p>Functions with a forced execution policy do not interact with the AI. The AI is neither queried nor trained.
	 * For this reason, these functions also do not need to return identifiers and can instead return null.</p>
	 *
	 * <p>This can be used as a way to statically declare global dependencies (or lack thereof). It can also be used to
	 * force transactions to execute optimistically, eliminating the overhead of the AI systems.</p>
	 *
	 * @return The {@link ExecutionPolicy} for this function, defaults to USE_AI
	 */
	default ExecutionPolicy getExecutionPolicy() { return ExecutionPolicy.USE_AI; }

	enum ExecutionPolicy { USE_AI, FORCE_OPTIMISTIC, FORCE_GLOBAL_BLOCK }

}