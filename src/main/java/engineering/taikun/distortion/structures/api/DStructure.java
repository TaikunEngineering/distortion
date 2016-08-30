package engineering.taikun.distortion.structures.api;

import engineering.taikun.distortion.serialization.api.ByteArray;
import engineering.taikun.distortion.store.api.KV;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;

public interface DStructure<BA extends ByteArray<BA>> {

	/**
	 * Get the backing KV
	 *
	 * @return The backing KV
	 */
	KV<BA> getKV();

	/**
	 * Change the backing KV
	 *
	 * @param kv The new KV
	 */
	void setKV(KV<BA> kv);

	/**
	 * Get the key used to lookup this structure from its parent KV
	 *
	 * @return The key used to lookup this structure
	 */
	ByteArray getKey();

	/**
	 * Change the remembered key
	 *
	 * @param key The new key
	 */
	void setKey(ByteArray key);

	/**
	 * Get the custom class name. Should return null if not set.
	 *
	 * @return Custom class name
	 */
	@Nullable String getCustomClass();

	/**
	 * Set the custom class name, null forbidden.
	 *
	 * @param className Custom class name
	 */
	void setCustomClass(@NotNull String className);

	/**
	 * Get the key where the remotes set is stored
	 *
	 * @return The key where the remotes set is stored
	 */
	ByteArray getRemotesKey();

	/**
	 * Destroys the structure and its contents
	 */
	void destroy();

	interface DCollectionStructure<T, BA extends ByteArray<BA>> extends Collection<T>, DStructure<BA> {
		/**
		 * Like {@link Collection#add}, but returns the inserted item's reflection, if compatible
		 */
		DStructure reflectingAdd(T t);
	}

	interface DMapStructure<K, V, BA extends ByteArray<BA>> extends Map<K, V>, DStructure<BA> {

		class PutReflection {
			public final DStructure key_reflection;
			public final DStructure value_reflection;

			public PutReflection(final DStructure key_reflection, final DStructure value_reflection) {
				this.key_reflection = key_reflection;
				this.value_reflection = value_reflection;
			}
		}

		/**
		 * <p>Like {@link Map#put}, but returns the inserted item's key/value reflections, if compatible</p>
		 *
		 * <p>Implementations may only return the value reflection if the key was previously mapped</p>
		 */
		PutReflection reflectingPut(K key, V value);
	}

}
