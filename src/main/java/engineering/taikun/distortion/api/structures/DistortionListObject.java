package engineering.taikun.distortion.api.structures;

import engineering.taikun.distortion.structures.api.DistortionObject;

import java.util.List;

/**
 * <p>This is one of the Distortion object classes that allows you to define custom classes compatible with
 * Distortion's object system. Instances of these classes are automatically persisted and restored by Distortion.</p>
 *
 * <p>Take note, there are limitations which you must take into consideration</p>
 *
 * <p><b>ALL STATE MUST</b> be managed by an internal {@link List}. If you construct the object yourself, you can use
 * whatever implementation you want (including read-only). If Distortion constructs this object, it will be backed by a
 * Distortion structure (where changes are pushed to Distortion's data store).</p>
 *
 * <p>Any fields, if present, will be ignored when the object is persisted</p>
 *
 * <p>All fields except the backing data structure will be <b>ZERO</b> when the object is restored. Even static
 * initializers are NOT run.</p>
 *
 * <p>Despite the <tt>final</tt> flag, the list in a restored object can be swapped out by Distortion at any time. You
 * cannot use class specific code.</p>
 *
 * <p>Note: No constructor is called when the class is instantiated. You are free to limit your class to custom
 * constructors.</p>
 *
 * <p>Warning: Distortion will yield an undecorated Distortion object if the custom class cannot be instantiated.</p>
 */
@SuppressWarnings({ "FieldMayBeFinal", "CanBeFinal" })
public abstract class DistortionListObject<T> extends DistortionObject {

	private List<T> backingList;

	protected DistortionListObject(final List<T> backingList) {
		this.backingList = backingList;
	}

	protected List<T> list() {
		return this.backingList;
	}
}
