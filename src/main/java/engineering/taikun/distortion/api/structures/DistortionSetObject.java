package engineering.taikun.distortion.api.structures;

import engineering.taikun.distortion.structures.api.DistortionObject;
import org.jetbrains.annotations.Nullable;

import java.util.Set;
import java.util.function.IntSupplier;

/**
 * <p>This is one of the Distortion object classes that allows you to define custom classes compatible with
 * Distortion's object system. Instances of these classes are automatically persisted and restored by Distortion.</p>
 *
 * <p>Custom Distortion set objects can configure their own concurrency levels. This is similar to a
 * ConcurrentHashMap, where the level is the theoretical max number of writing threads.</p>
 *
 * <p>Take note, there are limitations which you must take into consideration</p>
 *
 * <p><b>ALL STATE MUST</b> be managed by an internal {@link Set}. If you construct the object yourself, you can use
 * whatever implementation you want (including read-only). If Distortion constructs this object, it will be backed by a
 * Distortion structure (where changes are pushed to Distortion's data store).</p>
 *
 * <p>Any fields, if present, will be ignored when the object is persisted</p>
 *
 * <p>All fields except the backing data structure will be <b>ZERO</b> when the object is restored. Even static
 * initializers are NOT run.</p>
 *
 * <p>Despite the <tt>final</tt> flag, the set in a restored object can be swapped out by Distortion at any time. You
 * cannot use class specific code.</p>
 *
 * <p>Note: No constructor is called when the class is instantiated. You are free to limit your class to custom
 * constructors.</p>
 *
 * <p>Note: Unlike a ConcurrentHashMap, Distortion sets are not sharded, but are instead structured like a skip list.
 * Writes are distributed truly randomly, not based on hashcode. Overspeccing to hit a power-of-two or core count
 * provides no implicit benefit.</p>
 *
 * <p>Note: Not specifying a concurrency level will cause it to take on the global default.</p>
 *
 * <p>Note: A set with a concurrency level of 1 is an ordered set.</p>
 *
 * <p>Warning: Distortion will yield an undecorated Distortion object if the custom class cannot be instantiated.</p>
 */
@SuppressWarnings({ "FieldMayBeFinal", "CanBeFinal" })
public abstract class DistortionSetObject<T> extends DistortionObject {

	private Set<T> backingSet;
	private short concurrencyLevel;
	private final @Nullable IntSupplier conc_supplier;

	protected DistortionSetObject(final Set<T> backingSet) {
		this.backingSet = backingSet;
		this.concurrencyLevel = 0;
		this.conc_supplier = null;
	}

	protected DistortionSetObject(final Set<T> backingSet, final short concurrencyLevel) {
		this.backingSet = backingSet;
		this.concurrencyLevel = concurrencyLevel;
		this.conc_supplier = null;
	}

	protected Set<T> set() {
		return this.backingSet;
	}

	@SuppressWarnings("ConstantConditions")
	protected short getConcurrencyLevel() {
		if (this.concurrencyLevel == -1) {
			this.concurrencyLevel = (short) this.conc_supplier.getAsInt();
		}

		return this.concurrencyLevel;
	}
}
