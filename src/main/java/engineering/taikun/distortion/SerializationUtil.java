package engineering.taikun.distortion;

import engineering.taikun.distortion.Distortion.FJWT;
import engineering.taikun.distortion.api.structures.*;
import engineering.taikun.distortion.serialization.api.ByteArray;
import engineering.taikun.distortion.serialization.api.ByteArrayFactory;
import engineering.taikun.distortion.serialization.imp.ArrayWrapper;
import engineering.taikun.distortion.store.api.KV;
import engineering.taikun.distortion.structures.api.DStructure;
import engineering.taikun.distortion.structures.api.DStructure.DCollectionStructure;
import engineering.taikun.distortion.structures.api.DStructure.DMapStructure;
import engineering.taikun.distortion.structures.api.DStructure.DMapStructure.PutReflection;
import engineering.taikun.distortion.structures.api.DistortionObject;
import engineering.taikun.distortion.structures.api.Struct;
import engineering.taikun.distortion.structures.imp.*;
import org.jetbrains.annotations.Nullable;
import sun.misc.Unsafe;

import java.io.*;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

/**
 * The class where everything related to serialization, but independent of implementation details, is kept
 *
 * @param <BA> The {@link ByteArray} implementation type
 */
@SuppressWarnings({ "ConstantConditions", "UseOfSunClasses" })
public final class SerializationUtil<BA extends ByteArray<BA>> implements ByteArrayFactory<BA> {

	public static final ByteArray EMPTY_ARRAY = new ArrayWrapper(new byte[0]);
	public static final ByteArray FF = new ArrayWrapper(new byte[]{ (byte) 0xFF });

	public static final String[] EMPTY_STRING_AR = new String[0];

	public static final Unsafe unsafe;

	// Distortion object offsets
	static final long struct_field_offset;

	static final long list_field_offset;

	static final long map_field_offset;
	static final long map_conc_field_offset;
	static final long map_supplier_field_offset;

	static final long set_field_offset;
	static final long set_conc_field_offset;
	static final long set_supplier_field_offset;

	static final long navigable_map_field_offset;
	static final long navigable_map_conc_field_offset;
	static final long navigable_map_supplier_field_offset;

	static final long navigable_set_field_offset;
	static final long navigable_set_conc_field_offset;
	static final long navigable_set_supplier_field_offset;

	static {
		try {
			final Field unsafe_field = Unsafe.class.getDeclaredField("theUnsafe");
			unsafe_field.setAccessible(true);
			unsafe = (Unsafe) unsafe_field.get(null);

			// We use private to prevent the user from changing the backing structure, but _we_ need to change it

			final Field struct_field = DistortionStructObject.class.getDeclaredField("backingStruct");
			struct_field_offset = unsafe.objectFieldOffset(struct_field);

			final Field list_field = DistortionListObject.class.getDeclaredField("backingList");
			list_field_offset = unsafe.objectFieldOffset(list_field);

			final Field map_field = DistortionMapObject.class.getDeclaredField("backingMap");
			map_field_offset = unsafe.objectFieldOffset(map_field);
			final Field map_conc_field = DistortionMapObject.class.getDeclaredField("concurrencyLevel");
			map_conc_field_offset = unsafe.objectFieldOffset(map_conc_field);
			final Field map_supplier_field = DistortionMapObject.class.getDeclaredField("conc_supplier");
			map_supplier_field_offset = unsafe.objectFieldOffset(map_supplier_field);

			final Field set_field = DistortionSetObject.class.getDeclaredField("backingSet");
			set_field_offset = unsafe.objectFieldOffset(set_field);
			final Field set_conc_field = DistortionSetObject.class.getDeclaredField("concurrencyLevel");
			set_conc_field_offset = unsafe.objectFieldOffset(set_conc_field);
			final Field set_supplier_field = DistortionSetObject.class.getDeclaredField("conc_supplier");
			set_supplier_field_offset = unsafe.objectFieldOffset(set_supplier_field);

			final Field navigable_map_field = DistortionMapObject.class.getDeclaredField("backingMap");
			navigable_map_field_offset = unsafe.objectFieldOffset(navigable_map_field);
			final Field navigable_map_conc_field = DistortionMapObject.class.getDeclaredField("concurrencyLevel");
			navigable_map_conc_field_offset = unsafe.objectFieldOffset(navigable_map_conc_field);
			final Field navigable_map_supplier_field = DistortionMapObject.class.getDeclaredField("conc_supplier");
			navigable_map_supplier_field_offset = unsafe.objectFieldOffset(navigable_map_supplier_field);

			final Field navigable_set_field = DistortionSetObject.class.getDeclaredField("backingSet");
			navigable_set_field_offset = unsafe.objectFieldOffset(navigable_set_field);
			final Field navigable_set_conc_field = DistortionSetObject.class.getDeclaredField("concurrencyLevel");
			navigable_set_conc_field_offset = unsafe.objectFieldOffset(navigable_set_conc_field);
			final Field navigable_set_supplier_field = DistortionSetObject.class.getDeclaredField("conc_supplier");
			navigable_set_supplier_field_offset = unsafe.objectFieldOffset(navigable_set_supplier_field);

		} catch (final Exception e) {
			throw new RuntimeException("Distortion requires Unsafe to function", e);
		}
	}

	public final ByteArrayFactory<BA> factory;
	public final short defaultConcurrencyLevel;

	final Consumer<Boolean> set_thread_lock;
	final BooleanSupplier get_thread_lock;

	final BA null_BA;
	final BA true_BA;
	final BA false_BA;

	/**
	 * <p>Create a new SerializationUtil by wrapping a {@link ByteArrayFactory}</p>
	 *
	 * <p>The concurrency level describes how sharded shardable (all but DList) structures will be configured. A sane
	 * number is 1-4x the number of Distortion execution threads (configured when you create a Distortion instance).</p>
	 *
	 * <p>Be aware that very large concurrency levels can actually reduce performance by having structures create many
	 * unused, redundant sub-structures or by having them use many more layers of indirection than necessary.</p>
	 *
	 * <p>Min concurrency level is 1</p>
	 *
	 * @param factory A {@link ByteArrayFactory}
	 * @param defaultConcurrencyLevel The default concurrency level to use for DMaps and DSets
	 */
	public SerializationUtil(final ByteArrayFactory<BA> factory, final short defaultConcurrencyLevel) {
		if (defaultConcurrencyLevel < 1) {
			throw new IllegalArgumentException("defaultConcurrencyLevel must be at least 1");
		}

		this.factory = factory;
		this.defaultConcurrencyLevel = defaultConcurrencyLevel;

		this.set_thread_lock = FJWT.setThreadLock();
		this.get_thread_lock = FJWT.getTheadLock();

		this.null_BA = BA(new byte[]{ 0x01 });
		this.true_BA = BA(new byte[]{ 0x02 });
		this.false_BA = BA(new byte[]{ 0x03 });
	}

	/**
	 * <h1>This constructor should only be used when debugging SerializationUtil itself</h1>
	 */
	public SerializationUtil(
			final ByteArrayFactory<BA> factory, final short defaultConcurrencyLevel, final Consumer<Boolean> set_thread_lock,
			final BooleanSupplier get_thread_lock
	) {
		if (!Distortion.DEBUG) {
			throw new IllegalStateException("Not in debug mode");
		}

		this.factory = factory;
		this.defaultConcurrencyLevel = defaultConcurrencyLevel;

		this.set_thread_lock = set_thread_lock;
		this.get_thread_lock = get_thread_lock;

		this.null_BA = BA(new byte[]{ 0x01 });
		this.true_BA = BA(new byte[]{ 0x02 });
		this.false_BA = BA(new byte[]{ 0x03 });
	}

	@Override
	public BA allocate(final int length) {
		return this.factory.allocate(length);
	}

	/**
	 * <p>A utility method to get a {@link ByteArray} imp out of a byte[]</p>
	 *
	 * <p>Note: This should be used sparingly. Don't forget, if you have a byte[], you can use {@link ArrayWrapper} to
	 * wrap it.</p>
	 *
	 * @param array The byte array literal
	 * @return The {@link ByteArray} imp
	 */
	public BA BA(final byte[] array) {
		final BA temp = allocate(array.length);
		for (int i = 0; i < array.length; i++) {
			temp.write(i, array[i]);
		}
		return temp;
	}

	/**
	 * <p>A utility method to copy a generic {@link ByteArray} to the ByteArray imp</p>
	 *
	 * @param array The {@link ByteArray} to copy
	 * @return The {@link ByteArray} imp
	 */
	public BA BA(final ByteArray array) {
		final BA temp = allocate(array.length());
		for (int i = 0; i < array.length(); i++) {
			temp.write(i, array.read(i));
		}
		return temp;
	}

	/**
	 * <p>Combines the inputs by COPYING into a single {@link ByteArray} imp</p>
	 *
	 * <p>Accepts Byte, byte[], byte[][], ByteArray, ByteArray[]</p>
	 *
	 * @param objects bytes and ByteArrays
	 * @return A single {@link ByteArray} imp of the elements concatenated
	 */
	public BA compose(final Object... objects) {
		final BA buffer = this.factory.allocate(10);
		final int[] length = { 0 };

		composeHelper(buffer, length, objects);

		if (buffer.length() != length[0])
			buffer.resize(length[0]);

		return buffer;
	}

	private void composeHelper(final BA buffer, final int[] length, final Object[] objar) {
		for (final Object obj : objar) {
			if (obj instanceof Byte) {
				if (buffer.length() == length[0]) {
					buffer.resize(buffer.length() * 2);
				}
				buffer.write(length[0]++, (Byte) obj);
			} else if (obj instanceof byte[]) {
				final byte[] btarr = (byte[]) obj;
				if (length[0] + btarr.length > buffer.length()) {
					int new_buffer_size = buffer.length();
					while (new_buffer_size < length[0] + btarr.length) {
						new_buffer_size *= 2;
					}
					buffer.resize(new_buffer_size);
				}
				for (final byte bite : btarr) {
					buffer.write(length[0]++, bite);
				}
			} else if (obj instanceof ByteArray) {
				final ByteArray btarr = (ByteArray) obj;
				if (length[0] + btarr.length() > buffer.length()) {
					int new_buffer_size = buffer.length();
					while (new_buffer_size < length[0] + btarr.length()) {
						new_buffer_size *= 2;
					}
					buffer.resize(new_buffer_size);
				}
				for (int i = 0; i < btarr.length(); i++) {
					buffer.write(length[0]++, btarr.read(i));
				}
			} else if (obj instanceof Object[]) {
				composeHelper(buffer, length, (Object[]) obj);
			} else {
				throw new IllegalArgumentException();
			}
		}
	}

	static void kill(final DStructure structure) {
		if (Distortion.DEBUG) {
			if (!Thread.holdsLock(structure)) {
				throw new RuntimeException();
			}
		}

		structure.destroy();
		structure.setKV(null);
	}

	// todo stupid
	public static <T extends DistortionListObject> T export(final T object, final List list) {
		try {

			final T toreturn = (T) unsafe.allocateInstance(object.getClass());
			final List clone = new ArrayList((DList) list);
			unsafe.putObjectVolatile(toreturn, list_field_offset, clone);
			return toreturn;

		} catch (final InstantiationException e) {
			throw new RuntimeException(e);
		} catch (final ClassCastException ignored) {
			throw new RuntimeException("Cannot export objects which were not synthesized by Distortion");
		}
	}

	public static <T extends DistortionMapObject> T export(final T object, final Map map) {
		try {

			final T toreturn = (T) unsafe.allocateInstance(object.getClass());
			final Map clone = new HashMap((DMap) map);
			unsafe.putObject(toreturn, map_field_offset, clone);
			unsafe.putShortVolatile(toreturn, map_conc_field_offset, ((DMap) map).getConcurrencyLevel());
			return toreturn;

		} catch (final InstantiationException e) {
			throw new RuntimeException(e);
		} catch (final ClassCastException ignored) {
			throw new RuntimeException("Cannot export objects which were not synthesized by Distortion");
		}
	}

	public static <T extends DistortionSetObject> T export(final T object, final Set set) {
		try {

			final T toreturn = (T) unsafe.allocateInstance(object.getClass());
			final Set clone = new HashSet<>((DSet) set);
			unsafe.putObject(toreturn, set_field_offset, clone);
			unsafe.putObjectVolatile(toreturn, set_conc_field_offset, ((DSet) set).getConcurrencyLevel());
			return toreturn;

		} catch (final InstantiationException e) {
			throw new RuntimeException(e);
		} catch (final ClassCastException ignored) {
			throw new RuntimeException("Cannot export objects which were not synthesized by Distortion");
		}
	}

	public static List export(final List object) {
		try {
			return new ArrayList<>((DList) object);
		} catch (final ClassCastException ignored) {
			throw new RuntimeException("Cannot export objects which were not synthesized by Distortion");
		}
	}

	public static Map export(final Map object) {
		try {
			return new HashMap<>((DMap) object);
		} catch (final ClassCastException ignored) {
			throw new RuntimeException("Cannot export objects which were not synthesized by Distortion");
		}
	}

	public static Set export(final Set object) {
		try {
			return new HashSet<>((DSet) object);
		} catch (final ClassCastException ignored) {
			throw new RuntimeException("Cannot export objects which were not synthesized by Distortion");
		}
	}

	static class StructSpec {
		final @Nullable String custom_class;
		final int size;

		StructSpec(final @Nullable String custom_class, final int size) {
			this.custom_class = custom_class;
			this.size = size;
		}
	}

	static class ListSpec {
		final @Nullable String custom_class;

		ListSpec(final @Nullable String custom_class) {
			this.custom_class = custom_class;
		}
	}

	static class MapSpec {
		final @Nullable String custom_class;
		final short concurrency_level;

		MapSpec(final @Nullable String custom_class, final short concurrency_level) {
			this.custom_class = custom_class;
			this.concurrency_level = concurrency_level;
		}
	}

	static class SetSpec {
		final @Nullable String custom_class;
		final short concurrency_level;

		SetSpec(final @Nullable String custom_class, final short concurrency_level) {
			this.custom_class = custom_class;
			this.concurrency_level = concurrency_level;
		}
	}

	static class NavigableMapSpec {
		final @Nullable String custom_class;
		final short concurrency_level;
		final @Nullable Comparator comparator;

		NavigableMapSpec(
				final @Nullable String custom_class, final short concurrency_level, final @Nullable Comparator comparator
		) {
			this.custom_class = custom_class;
			this.concurrency_level = concurrency_level;
			this.comparator = comparator;
		}
	}

	static class NavigableSetSpec {
		final @Nullable String custom_class;
		final short concurrency_level;
		final @Nullable Comparator comparator;

		NavigableSetSpec(
				final @Nullable String custom_class, final short concurrency_level, final @Nullable Comparator comparator
		) {
			this.custom_class = custom_class;
			this.concurrency_level = concurrency_level;
			this.comparator = comparator;
		}
	}

	static Object spec(final DStructure structure) {
		if (structure instanceof DStruct) {

			return new StructSpec(structure.getCustomClass(), ((DStruct) structure).size());

		} else if (structure instanceof DList) {

			return new ListSpec(structure.getCustomClass());

		} else if (structure instanceof DMap) {

			return new MapSpec(structure.getCustomClass(), ((DMap) structure).getConcurrencyLevel());

		} else if (structure instanceof DSet) {

			return new SetSpec(structure.getCustomClass(), ((DSet) structure).getConcurrencyLevel());

		} else if (structure instanceof DNavigableMap) {

			return new NavigableMapSpec(
					structure.getCustomClass(), ((DNavigableMap) structure).getConcurrencyLevel(),
					((DNavigableMap) structure).comparator()
			);

		} else if (structure instanceof DNavigableSet) {

			return new NavigableSetSpec(
					structure.getCustomClass(), ((DNavigableSet) structure).getConcurrencyLevel(),
					((DNavigableSet) structure).comparator()
			);

		} else {
			throw new IllegalArgumentException("Unkown structure type");
		}
	}

	static byte code(final DStructure structure) {
		if (structure instanceof DStruct) {

			return 0x71;

		} else if (structure instanceof DList) {

			return 0x72;

		} else if (structure instanceof DMap) {

			return 0x73;

		} else if (structure instanceof DSet) {

			return 0x74;

		} else if (structure instanceof DNavigableMap) {

			return 0x75;

		} else if (structure instanceof DNavigableSet) {

			return 0x76;

		} else {
			throw new IllegalArgumentException("Unkown structure type");
		}
	}

	public class SerializationContext implements Closeable {

		/*
		 * A few words on the concurrency support of this class
		 *
		 * Since the client code can be multithreaded and racey, we must have a modicum of protection against
		 * self-corruption.
		 *
		 * This ends up being pretty trivial as we only have two hashmaps to deal with, however the state of the KV must
		 * also be considered. If we were only to protect our state, it would be trivial for the client to leave garbage in
		 * the KV. Blind writes could overwrite structure descriptors, client could delete a structure being moved, etc.
		 *
		 * Since we have full access to every read/write the client calls, we could in theory make their code safe for them
		 * by using our own read/write lock.
		 *
		 * But this is overly safe. I feel anybody who wants to still do threading after using Distortion is well aware of
		 * the difficulties. Therefore, I decided to implement the minimum necessary to get JVM heap-like semantics.
		 *
		 *  - No garbage -> all writes are serialized
		 *  - Incomplete object state allowed -> object moves start as a write, demote to a read when moving
		 *
		 * Basically, we do not care to correct for bad client code, but we must ensure to protect enough that the KV is
		 * always fully reachable.
		 *
		 */

		final KV<BA> root;

		final StampedLock lock = new StampedLock();

		final HashMap<ByteArray, WeakReference<DStructure>> live_structures = new HashMap<>();
		// TODO undo public
		public final HashMap<ByteArray, DStructure> tagged_structures = new HashMap<>();

		@SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
		int tag_pointed = 0;
		final IdentityHashMap<DStructure, ByteArray> forwarded = new IdentityHashMap<>();
		@SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
		boolean no_unpack = false;

		public SerializationContext(final KV<BA> root) {
			this.root = root;
		}

		/**
		 * <p>Serialize an object to a {@link ByteArray} imp</p>
		 *
		 * <p>Distortion supports serializing a limited set of atoms</p>
		 *
		 * <p>Since this set is small and not extensible, a helper function is simpler than wrapping the primitives with
		 * serialization logic</p>
		 *
		 * <p>Note: Array length is encoded to prevent prefix attacks</p>
		 *
		 * <p>Note: There is no strict corresponding deserialize. Use {@link #unpack} with a <tt>null</tt> source to
		 * deserialize.</p>
		 *
		 * <p>Supported atoms:</p>
		 * <ul>
		 *   <li><tt>null</tt></li>
		 *   <li>Boolean</li>>
		 *   <li>boolean[]</li>
		 *   <li>Byte</li>
		 *   <li>byte[]</li>
		 *   <li>Short</li>
		 *   <li>short[]</li>
		 *   <li>Character</li>
		 *   <li>char[]</li>
		 *   <li>Integer</li>
		 *   <li>int[]</li>
		 *   <li>Long</li>
		 *   <li>long[]</li>
		 *   <li>Float</li>
		 *   <li>float[]</li>
		 *   <li>Double</li>
		 *   <li>double[]</li>
		 *   <li>String</li>
		 *   <li>String[]</li>
		 * </ul>
		 *
		 * @see #unpack
		 *
		 * @param obj The object to serialize
		 * @return The {@link ByteArray} imp serialization, null if unhandled
		 */
		public @Nullable BA serialize(final @Nullable Object obj) {
			if (obj == null) {

				return SerializationUtil.this.null_BA;

			} else if (obj instanceof Boolean) {

				if ((Boolean) obj) {
					return SerializationUtil.this.true_BA;
				} else {
					return SerializationUtil.this.false_BA;
				}

			} else if (obj instanceof Byte) {

				return compose((byte) 0x04, obj);

			} else if (obj instanceof byte[]) {

				final byte[] bytear = (byte[]) obj;

				if (bytear.length < 256) {
					return compose((byte) 0x05, (byte) (bytear.length - 128), obj);
				} else if (bytear.length < 65536) {
					return compose((byte) 0x06, new ShortUnion((short) (bytear.length - 32768)).getBytes(), obj);
				} else {
					return compose((byte) 0x07, new IntUnion(bytear.length).getBytes(), obj);
				}

			} else if (obj instanceof Short) {

				return compose((byte) 0x08, new ShortUnion((Short) obj).getBytes());

			} else if (obj instanceof short[]) {

				final short[] shortar = (short[]) obj;
				final byte[][] temp_array = new byte[shortar.length][];

				for (int i = 0; i < shortar.length; i++) {
					temp_array[i] = new ShortUnion(shortar[i]).getBytes();
				}

				if (shortar.length < 256) {
					return compose((byte) 0x09, (byte) (shortar.length - 128), temp_array);
				} else if (shortar.length < 65536) {
					return compose((byte) 0x0A, new ShortUnion((short) (shortar.length - 32768)).getBytes(), temp_array);
				} else {
					return compose((byte) 0x0B, new IntUnion(shortar.length).getBytes(), temp_array);
				}

			} else if (obj instanceof Character) {

				return compose((byte) 0x0C, new ShortUnion((short) ((Character) obj - 32768)).getBytes());

			} else if (obj instanceof char[]) {

				final char[] charar = (char[]) obj;
				final byte[][] temp_array = new byte[charar.length][];

				for (int i = 0; i < charar.length; i++) {
					temp_array[i] = new ShortUnion((short) (charar[i] - 32768)).getBytes();
				}

				if (charar.length < 256) {
					return compose((byte) 0x0D, (byte) (charar.length - 128), temp_array);
				} else if (charar.length < 65536) {
					return compose((byte) 0x0E, new ShortUnion((short) (charar.length - 32768)).getBytes(), temp_array);
				} else {
					return compose((byte) 0x0F, new IntUnion(charar.length).getBytes(), temp_array);
				}

			} else if (obj instanceof Integer) {

				return compose((byte) 0x10, new IntUnion((Integer) obj).getBytes());

			} else if (obj instanceof int[]) {

				final int[] intar = (int[]) obj;
				final byte[][] temp_array = new byte[intar.length][];

				for (int i = 0; i < intar.length; i++) {
					temp_array[i] = new IntUnion(intar[i]).getBytes();
				}

				if (intar.length < 256) {
					return compose((byte) 0x11, (byte) (intar.length - 128), temp_array);
				} else if (intar.length < 65536) {
					return compose((byte) 0x12, new ShortUnion((short) (intar.length - 32768)).getBytes(), temp_array);
				} else {
					return compose((byte) 0x13, new IntUnion(intar.length).getBytes(), temp_array);
				}

			} else if (obj instanceof Long) {

				return compose((byte) 0x14, new LongUnion((Long) obj).getBytes());

			} else if (obj instanceof long[]) {

				final long[] longar = (long[]) obj;
				final byte[][] temp_array = new byte[longar.length][];

				for (int i = 0; i < longar.length; i++) {
					temp_array[i] = new LongUnion(longar[i]).getBytes();
				}

				if (longar.length < 256) {
					return compose((byte) 0x15, (byte) (longar.length - 128), temp_array);
				} else if (longar.length < 65536) {
					return compose((byte) 0x16, new ShortUnion((short) (longar.length - 32768)).getBytes(), temp_array);
				} else {
					return compose((byte) 0x17, new IntUnion(longar.length).getBytes(), temp_array);
				}

			} else if (obj instanceof Float) {

				return compose((byte) 0x18, new IntUnion(Float.floatToRawIntBits((Float) obj)).getBytes());

			} else if (obj instanceof float[]) {

				final float[] floatar = (float[]) obj;
				final byte[][] temp_array = new byte[floatar.length][];

				for (int i = 0; i < floatar.length; i++) {
					temp_array[i] = new IntUnion(Float.floatToRawIntBits(floatar[i])).getBytes();
				}

				if (floatar.length < 256) {
					return compose((byte) 0x19, (byte) (floatar.length - 128), temp_array);
				} else if (floatar.length < 65536) {
					return compose((byte) 0x1A, new ShortUnion((short) (floatar.length - 32768)).getBytes(), temp_array);
				} else {
					return compose((byte) 0x1B, new IntUnion(floatar.length).getBytes(), temp_array);
				}

			} else if (obj instanceof Double) {

				return compose((byte) 0x1C, new LongUnion(Double.doubleToRawLongBits((Double) obj)).getBytes());

			} else if (obj instanceof double[]) {

				final double[] dublar = (double[]) obj;
				final byte[][] temp_array = new byte[dublar.length][];

				for (int i = 0; i < dublar.length; i++) {
					temp_array[i] = new LongUnion(Double.doubleToRawLongBits(dublar[i])).getBytes();
				}

				if (dublar.length < 256) {
					return compose((byte) 0x1D, (byte) (dublar.length - 128), temp_array);
				} else if (dublar.length < 65536) {
					return compose((byte) 0x1E, new ShortUnion((short) (dublar.length - 32768)).getBytes(), temp_array);
				} else {
					return compose((byte) 0x1F, new IntUnion(dublar.length).getBytes(), temp_array);
				}

			} else if (obj instanceof String) {

				final String string = (String) obj;

				if (string.length() < 256) {
					return compose((byte) 0x20, (byte) (string.length() - 128), string.getBytes(StandardCharsets.UTF_8));
				} else if (string.length() < 65536) {
					return compose(
							(byte) 0x21, new ShortUnion((short) (string.length() - 32768)).getBytes(),
							string.getBytes(StandardCharsets.UTF_8)
					);
				} else {
					return compose(
							(byte) 0x22, new IntUnion(string.length()).getBytes(), string.getBytes(StandardCharsets.UTF_8)
					);
				}

			} else if (obj instanceof String[]) {

				final String[] stringar = (String[]) obj;
				final Object[] dump = new Object[stringar.length * 2];

				int length = 0;
				int max = 0;

				for (final String string : stringar) {
					final byte[] stringbytes = string.getBytes(StandardCharsets.UTF_8);

					length = Math.addExact(length, stringbytes.length);
					max = Math.max(max, stringbytes.length);
				}

				for (int i = 0; i < stringar.length; i++) {
					final byte[] stringbytes = stringar[i].getBytes(StandardCharsets.UTF_8);

					if (max < 256) {
						dump[2 * i] = (byte) (stringbytes.length - 128);
					} else if (max < 65536) {
						dump[2 * i] = new ShortUnion((short) (stringbytes.length - 32768)).getBytes();
					} else {
						dump[2 * i] = new IntUnion(stringbytes.length).getBytes();
					}

					dump[2 * i + 1] = stringbytes;
				}

				if (max < 256) {
					return compose((byte) 0x23, new IntUnion(length).getBytes(), dump);
				} else if (max < 65536) {
					return compose((byte) 0x24, new IntUnion(length).getBytes(), dump);
				} else {
					return compose((byte) 0x25, new IntUnion(length).getBytes(), dump);
				}

			} else if (obj instanceof ByteArray) {

				return compose((byte) 0x26, obj);

			} else {
				return null;
			}
		}

		private Object deserialize(final ByteArray bytes) {
			switch (bytes.read(0)) {
				case 0x01:
					return null;
				case 0x02:
					return true;
				case 0x03:
					return false;

				case 0x04:
					return bytes.read(1);
				case 0x05: {
					final byte[] toreturn = new byte[bytes.length() - 2];
					for (int i = 0; i < toreturn.length; i++) {
						toreturn[i] = bytes.read(i + 2);
					}
					return toreturn;
				}
				case 0x06: {
					final byte[] toreturn = new byte[bytes.length() - 3];
					for (int i = 0; i < toreturn.length; i++) {
						toreturn[i] = bytes.read(i + 3);
					}
					return toreturn;
				}
				case 0x07: {
					final byte[] toreturn = new byte[bytes.length() - 5];
					for (int i = 0; i < toreturn.length; i++) {
						toreturn[i] = bytes.read(i + 5);
					}
					return toreturn;
				}

				case 0x08:
					return new ShortUnion(bytes.read(1), bytes.read(2)).getShort();
				case 0x09: {
					final short[] toreturn = new short[(bytes.length() - 2) / 2];
					for (int i = 0; i < toreturn.length; i++) {
						toreturn[i] = new ShortUnion(bytes.read(i * 2 + 2), bytes.read(i * 2 + 3)).getShort();
					}
					return toreturn;
				}
				case 0x0A: {
					final short[] toreturn = new short[(bytes.length() - 3) / 2];
					for (int i = 0; i < toreturn.length; i++) {
						toreturn[i] = new ShortUnion(bytes.read(i * 2 + 3), bytes.read(i * 2 + 4)).getShort();
					}
					return toreturn;
				}
				case 0x0B: {
					final short[] toreturn = new short[(bytes.length() - 5) / 2];
					for (int i = 0; i < toreturn.length; i++) {
						toreturn[i] = new ShortUnion(bytes.read(i * 2 + 5), bytes.read(i * 2 + 6)).getShort();
					}
					return toreturn;
				}

				case 0x0C:
					return (char) (new ShortUnion(bytes.read(1), bytes.read(2)).getShort() + 32768);
				case 0x0D: {
					final char[] toreturn = new char[(bytes.length() - 2) / 2];
					for (int i = 0; i < toreturn.length; i++) {
						toreturn[i] = (char) (new ShortUnion(bytes.read(i * 2 + 2), bytes.read(i * 2 + 3)).getShort() + 32768);
					}
					return toreturn;
				}
				case 0x0E: {
					final char[] toreturn = new char[(bytes.length() - 3) / 2];
					for (int i = 0; i < toreturn.length; i++) {
						toreturn[i] = (char) (new ShortUnion(bytes.read(i * 2 + 3), bytes.read(i * 2 + 4)).getShort() + 32768);
					}
					return toreturn;
				}
				case 0x0F: {
					final char[] toreturn = new char[(bytes.length() - 5) / 2];
					for (int i = 0; i < toreturn.length; i++) {
						toreturn[i] = (char) (new ShortUnion(bytes.read(i * 2 + 5), bytes.read(i * 2 + 6)).getShort() + 32768);
					}
					return toreturn;
				}

				case 0x10:
					return new IntUnion(bytes.read(1), bytes.read(2), bytes.read(3), bytes.read(4)).getInt();
				case 0x11: {
					final int[] toreturn = new int[(bytes.length() - 2) / 4];
					for (int i = 0; i < toreturn.length; i++) {
						toreturn[i] = new IntUnion(
								bytes.read(i * 4 + 2), bytes.read(i * 4 + 3), bytes.read(i * 4 + 4), bytes.read(i * 4 + 5)
						).getInt();
					}
					return toreturn;
				}
				case 0x12: {
					final int[] toreturn = new int[(bytes.length() - 3) / 4];
					for (int i = 0; i < toreturn.length; i++) {
						toreturn[i] = new IntUnion(
								bytes.read(i * 4 + 3), bytes.read(i * 4 + 4), bytes.read(i * 4 + 5), bytes.read(i * 4 + 6)
						).getInt();
					}
					return toreturn;
				}
				case 0x13: {
					final int[] toreturn = new int[(bytes.length() - 5) / 4];
					for (int i = 0; i < toreturn.length; i++) {
						toreturn[i] = new IntUnion(
								bytes.read(i * 4 + 5), bytes.read(i * 4 + 6), bytes.read(i * 4 + 7), bytes.read(i * 4 + 8)
						).getInt();
					}
					return toreturn;
				}

				case 0x14:
					return new LongUnion(
							bytes.read(1), bytes.read(2), bytes.read(3), bytes.read(4),
							bytes.read(5), bytes.read(6), bytes.read(7), bytes.read(8)
					).getLong();
				case 0x15: {
					final long[] toreturn = new long[(bytes.length() - 2) / 8];
					for (int i = 0; i < toreturn.length; i++) {
						toreturn[i] = new LongUnion(
								bytes.read(i * 8 + 2), bytes.read(i * 8 + 3), bytes.read(i * 8 + 4), bytes.read(i * 8 + 5),
								bytes.read(i * 8 + 6), bytes.read(i * 8 + 7), bytes.read(i * 8 + 8), bytes.read(i * 8 + 9)
						).getLong();
					}
					return toreturn;
				}
				case 0x16: {
					final long[] toreturn = new long[(bytes.length() - 3) / 8];
					for (int i = 0; i < toreturn.length; i++) {
						toreturn[i] = new LongUnion(
								bytes.read(i * 8 + 3), bytes.read(i * 8 + 4), bytes.read(i * 8 + 5), bytes.read(i * 8 + 6),
								bytes.read(i * 8 + 7), bytes.read(i * 8 + 8), bytes.read(i * 8 + 9), bytes.read(i * 8 + 10)
						).getLong();
					}
					return toreturn;
				}
				case 0x17: {
					final long[] toreturn = new long[(bytes.length() - 5) / 8];
					for (int i = 0; i < toreturn.length; i++) {
						toreturn[i] = new LongUnion(
								bytes.read(i * 8 + 5), bytes.read(i * 8 + 6), bytes.read(i * 8 + 7), bytes.read(i * 8 + 8),
								bytes.read(i * 8 + 9), bytes.read(i * 8 + 10), bytes.read(i * 8 + 11), bytes.read(i * 8 + 12)
						).getLong();
					}
					return toreturn;
				}

				case 0x18:
					return Float.intBitsToFloat(new IntUnion(bytes.read(1), bytes.read(2), bytes.read(3), bytes.read(4)).getInt());
				case 0x19: {
					final float[] toreturn = new float[(bytes.length() - 2) / 4];
					for (int i = 0; i < toreturn.length; i++) {
						toreturn[i] = Float.intBitsToFloat(new IntUnion(
								bytes.read(i * 4 + 2), bytes.read(i * 4 + 3), bytes.read(i * 4 + 4), bytes.read(i * 4 + 5)
						).getInt());
					}
					return toreturn;
				}
				case 0x1A: {
					final float[] toreturn = new float[(bytes.length() - 3) / 4];
					for (int i = 0; i < toreturn.length; i++) {
						toreturn[i] = Float.intBitsToFloat(new IntUnion(
								bytes.read(i * 4 + 3), bytes.read(i * 4 + 4), bytes.read(i * 4 + 5), bytes.read(i * 4 + 6)
						).getInt());
					}
					return toreturn;
				}
				case 0x1B: {
					final float[] toreturn = new float[(bytes.length() - 5) / 4];
					for (int i = 0; i < toreturn.length; i++) {
						toreturn[i] = Float.intBitsToFloat(new IntUnion(
								bytes.read(i * 4 + 5), bytes.read(i * 4 + 6), bytes.read(i * 4 + 7), bytes.read(i * 4 + 8)
						).getInt());
					}
					return toreturn;
				}

				case 0x1C:
					return Double.longBitsToDouble(new LongUnion(
							bytes.read(1), bytes.read(2), bytes.read(3), bytes.read(4),
							bytes.read(5), bytes.read(6), bytes.read(7), bytes.read(8)
					).getLong());
				case 0x1D: {
					final double[] toreturn = new double[(bytes.length() - 2) / 8];
					for (int i = 0; i < toreturn.length; i++) {
						toreturn[i] = Double.longBitsToDouble(new LongUnion(
								bytes.read(i * 8 + 2), bytes.read(i * 8 + 3), bytes.read(i * 8 + 4), bytes.read(i * 8 + 5),
								bytes.read(i * 8 + 6), bytes.read(i * 8 + 7), bytes.read(i * 8 + 8), bytes.read(i * 8 + 9)
						).getLong());
					}
					return toreturn;
				}
				case 0x1E: {
					final double[] toreturn = new double[(bytes.length() - 3) / 8];
					for (int i = 0; i < toreturn.length; i++) {
						toreturn[i] = Double.longBitsToDouble(new LongUnion(
								bytes.read(i * 8 + 3), bytes.read(i * 8 + 4), bytes.read(i * 8 + 5), bytes.read(i * 8 + 6),
								bytes.read(i * 8 + 7), bytes.read(i * 8 + 8), bytes.read(i * 8 + 9), bytes.read(i * 8 + 10)
						).getLong());
					}
					return toreturn;
				}
				case 0x1F: {
					final double[] toreturn = new double[(bytes.length() - 5) / 8];
					for (int i = 0; i < toreturn.length; i++) {
						toreturn[i] = Double.longBitsToDouble(new LongUnion(
								bytes.read(i * 8 + 5), bytes.read(i * 8 + 6), bytes.read(i * 8 + 7), bytes.read(i * 8 + 8),
								bytes.read(i * 8 + 9), bytes.read(i * 8 + 10), bytes.read(i * 8 + 11), bytes.read(i * 8 + 12)
						).getLong());
					}
					return toreturn;
				}

				case 0x20: {
					final byte[] toreturn = new byte[bytes.length() - 2];
					for (int i = 2; i < bytes.length(); i++) {
						toreturn[i-2] = bytes.read(i);
					}
					return new String(toreturn, StandardCharsets.UTF_8);
				}
				case 0x21: {
					final byte[] toreturn = new byte[bytes.length() - 3];
					for (int i = 3; i < bytes.length(); i++) {
						toreturn[i-3] = bytes.read(i);
					}
					return new String(toreturn, StandardCharsets.UTF_8);
				}
				case 0x22: {
					final byte[] toreturn = new byte[bytes.length() - 5];
					for (int i = 5; i < bytes.length(); i++) {
						toreturn[i-5] = bytes.read(i);
					}
					return new String(toreturn, StandardCharsets.UTF_8);
				}

				case 0x23: {
					final ArrayList<String> toreturn = new ArrayList<>();
					for (int i = 5; i < bytes.length();) {
						final int bytear_length = bytes.read(i) + 128;
						toreturn.add(new String(bytes.slice(i + 1, i + 1 + bytear_length).toArray(), StandardCharsets.UTF_8));
						i += 1 + bytear_length;
					}
					return toreturn.toArray(EMPTY_STRING_AR);
				}
				case 0x24: {
					final ArrayList<String> toreturn = new ArrayList<>();
					for (int i = 5; i < bytes.length();) {
						final int bytear_length = new ShortUnion(bytes.read(i), bytes.read(i + 1)).getShort() + 32768;
						toreturn.add(new String(bytes.slice(i + 2, i + 2 + bytear_length).toArray(), StandardCharsets.UTF_8));
						i += 2 + bytear_length;
					}
					return toreturn.toArray(EMPTY_STRING_AR);
				}
				case 0x25: {
					final ArrayList<String> toreturn = new ArrayList<>();
					for (int i = 5; i < bytes.length();) {
						final int bytear_length
								= new IntUnion(bytes.read(i), bytes.read(i + 1), bytes.read(i + 2), bytes.read(i + 3)).getInt();
						toreturn.add(new String(bytes.slice(i + 4, i + 4 + bytear_length).toArray(), StandardCharsets.UTF_8));
						i += 4 + bytear_length;
					}
					return toreturn.toArray(EMPTY_STRING_AR);
				}

				case 0x026: {
					return bytes.slice(1, bytes.length());
				}

				default:
					throw new IllegalArgumentException();
			}
		}

		/**
		 * <p>Unpack a {@link ByteArray} to its original object. If the passed ByteArray was a Distortion data structure
		 * descriptor, this will also use the passed {@link KV} to reinstate the structure.</p>
		 *
		 * <p>This method is used to reinstate objects serialized and/or stored by the {@link #serialize} and
		 * {@link #store(KV, ByteArray, Object)}/{@link #store(KV, ByteArray, ByteArray, Object)} methods</p>
		 *
		 * @see #serialize
		 * @see #store(KV, ByteArray, Object)
		 * @see #store(KV, ByteArray, ByteArray, Object)
		 *
		 * @param source The optional {@link KV} to pull structure data from
		 * @param bytes The {@link ByteArray} containing or pointing to the object
		 * @return The reinstated object
		 */
		public @Nullable Object unpack(final @Nullable KV<BA> source, final ByteArray bytes) {
			if (Integer.compareUnsigned(bytes.read(0), 0x30) == -1) {
				return deserialize(bytes);
			}

			switch (bytes.read(0)) {
				case 0x70:
					throw new RuntimeException("Attempted to unpack at a structure boundary point");
				case 0x71:   // DStruct
				case 0x72:   // DList
				case 0x73:   // DMap
				case 0x74:   // DSet
				case 0x75:   // DNavigableMap
				case 0x76: { // DNavigableSet
					final ByteArray subkey = bytes.slice(1, bytes.length());
					final KV<BA> drilledKV = source.drill(compose((byte) 0x70, subkey));

					final boolean do_lock = !SerializationUtil.this.get_thread_lock.getAsBoolean();

					if (do_lock) {
						final long opti_read_stamp = this.lock.tryOptimisticRead();
						final WeakReference<DStructure> opti_weak_ref = this.live_structures.get(drilledKV.getPrefix());
						final DStructure opti_read = opti_weak_ref == null ? null : opti_weak_ref.get();

						if (opti_read != null && this.lock.validate(opti_read_stamp)) {
							return convert(opti_read);
						}
					}

					final long write_stamp = do_lock ? this.lock.writeLock() : 0;
					try {
						final WeakReference<DStructure> weak_ref = this.live_structures.get(drilledKV.getPrefix());
						final DStructure read = weak_ref == null ? null : weak_ref.get();

						if (read != null) {
							return convert(read);
						}

						final DStructure toreturn;
						switch (bytes.read(0)) {
							case 0x71:
								toreturn = new DStruct(drilledKV, subkey, SerializationUtil.this, this);
								break;
							case 0x72:
								toreturn = new DList(drilledKV, subkey, SerializationUtil.this, this, false);
								break;
							case 0x73:
								toreturn = new DMap(drilledKV, subkey, SerializationUtil.this, this);
								break;
							case 0x74:
								toreturn = new DSet(new DMap(drilledKV, subkey, SerializationUtil.this, this));
								break;
							case 0x75:
								toreturn = new DNavigableMap(drilledKV, subkey, SerializationUtil.this, this);
								break;
							case 0x76:
								toreturn = new DNavigableSet(new DNavigableMap(drilledKV, subkey, SerializationUtil.this, this));
								break;
							default: throw new RuntimeException();
						}

						this.live_structures.put(drilledKV.getPrefix(), new WeakReference<>(toreturn));

						return convert(toreturn);

					} finally {
						if (do_lock) {
							this.lock.unlockWrite(write_stamp);
						}
					}
				}
				case (byte) 0xF0: { // pointer
					final Pointer pointer = new Pointer(bytes.slice(1, bytes.length()), false);

					final Object toreturn = unpack(this.root.drill(pointer.kv), pointer.descriptor);

					if (SerializationUtil.this.get_thread_lock.getAsBoolean() && this.tag_pointed > 0) {
						this.forwarded.put(
								(DStructure) strip(toreturn),
								compose(new ShortUnion(short_(source.getPrefix().length())).getBytes(), source.getPrefix(), pointer.key)
						);
					}

					return toreturn;
				}
				default:
					throw new IllegalArgumentException();
			}
		}

		private DSet unpackRemote(final KV<BA> source, final ByteArray bytes) {

			if (Distortion.DEBUG) {
				if (!SerializationUtil.this.get_thread_lock.getAsBoolean()) {
					throw new RuntimeException();
				}
			}

			// should be a DSet
			if (bytes.read(0) == 0x74) {

				final ByteArray subkey = bytes.slice(1, bytes.length());
				final KV<BA> drilledKV = source.drill(compose((byte) 0x70, subkey));

				final DStructure toreturn = new DSet(new DMap(drilledKV, subkey, SerializationUtil.this, this));

				return (DSet) toreturn;

			} else {
				throw new IllegalArgumentException();
			}
		}

		private @Nullable DStructure structure(final KV<BA> source, final ByteArray bytes) {

			if (Distortion.DEBUG) {
				if (!SerializationUtil.this.get_thread_lock.getAsBoolean()) {
					throw new RuntimeException();
				}
			}

			switch (bytes.read(0)) {
				case 0x70:
					throw new RuntimeException("Attempted to unpack at a structure boundary point");
				case 0x71:   // DStruct
				case 0x72:   // DList
				case 0x73:   // DMap
				case 0x74:   // DSet
				case 0x75:   // DNavigableMap
				case 0x76: { // DNavigableSet
					final ByteArray subkey = bytes.slice(1, bytes.length());
					final KV<BA> drilledKV = source.drill(compose((byte) 0x70, subkey));

					final WeakReference<DStructure> weak_ref = this.live_structures.get(drilledKV.getPrefix());
					final DStructure read = weak_ref == null ? null : weak_ref.get();

					if (read != null) {
						return read;
					}

					final DStructure toreturn;
					switch (bytes.read(0)) {
						case 0x71:
							toreturn = new DStruct(drilledKV, subkey, SerializationUtil.this, this);
							break;
						case 0x72:
							toreturn = new DList(drilledKV, subkey, SerializationUtil.this, this, false);
							break;
						case 0x73:
							toreturn = new DMap(drilledKV, subkey, SerializationUtil.this, this);
							break;
						case 0x74:
							toreturn = new DSet(new DMap(drilledKV, subkey, SerializationUtil.this, this));
							break;
						case 0x75:
							toreturn = new DNavigableMap(drilledKV, subkey, SerializationUtil.this, this);
							break;
						case 0x76:
							toreturn = new DNavigableSet(new DNavigableMap(drilledKV, subkey, SerializationUtil.this, this));
							break;
						default: throw new RuntimeException();
					}

					return toreturn;
				}
				default:
					throw new IllegalArgumentException();
			}
		}

		public @Nullable Object unpackAndDestroy(final @Nullable KV<BA> source, final ByteArray bytes) {

			if (SerializationUtil.this.get_thread_lock.getAsBoolean() && this.no_unpack) {
				return null;
			}

			if (Integer.compareUnsigned(bytes.read(0), 0x30) == -1) {
				return deserialize(bytes);
			}

			final boolean do_lock = !SerializationUtil.this.get_thread_lock.getAsBoolean();

			switch (bytes.read(0)) {
				case 0x70: {
					throw new RuntimeException("Attempted to unpack at a structure boundary point");
				}
				case 0x71:   // DStruct
				case 0x72:   // DList
				case 0x73:   // DMap
				case 0x74:   // DSet
				case 0x75:   // DNavigableMap
				case 0x76: { // DNavigableSet
					final ByteArray subkey = bytes.slice(1, bytes.length());
					final KV<BA> drilledKV = source.drill(compose((byte) 0x70, subkey));

					final long write_stamp = do_lock ? this.lock.writeLock() : 0;
					if (do_lock) SerializationUtil.this.set_thread_lock.accept(true);
					try {

						final WeakReference<DStructure> weak_ref = this.live_structures.remove(drilledKV.getPrefix());
						final DStructure live = weak_ref == null ? null : weak_ref.get();
						DStructure temp = live;

						if (temp == null) {
							switch (bytes.read(0)) {
								case 0x71:
									temp = new DStruct(drilledKV, subkey, SerializationUtil.this, this);
									break;
								case 0x72:
									temp = new DList(drilledKV, subkey, SerializationUtil.this, this, false);
									break;
								case 0x73:
									temp = new DMap(drilledKV, subkey, SerializationUtil.this, this);
									break;
								case 0x74:
									temp = new DSet(new DMap(drilledKV, subkey, SerializationUtil.this, this));
									break;
								case 0x75:
									temp = new DNavigableMap(drilledKV, subkey, SerializationUtil.this, this);
									break;
								case 0x76:
									temp = new DNavigableSet(new DNavigableMap(drilledKV, subkey, SerializationUtil.this, this));
									break;
								default: throw new RuntimeException();
							}
						}

						this.tagged_structures.put(drilledKV.getPrefix(), temp);

						return convert(temp);

					} finally {
						if (do_lock) {
							SerializationUtil.this.set_thread_lock.accept(false);
							this.lock.unlockWrite(write_stamp);
						}
					}
				}
				case (byte) 0xF0: { // pointer
					// need to remove remote

					final Pointer pointer = new Pointer(bytes.slice(1, bytes.length()), false);

					final KV<BA> drilledKV = this.root.drill(compose(pointer.kv, (byte) 0x70, pointer.descriptor.slice(1, pointer.descriptor.length())));

					final long write_stamp = do_lock ? this.lock.writeLock() : 0;
					if (do_lock) SerializationUtil.this.set_thread_lock.accept(true);
					try {

						final WeakReference<DStructure> weak_ref = this.live_structures.get(drilledKV.getPrefix());
						DStructure structure = weak_ref == null ? null : weak_ref.get();

						if (structure == null) {
							structure = structure(this.root.drill(pointer.kv), pointer.descriptor);
						}

						synchronized (structure) {
							final Set<ByteArray> remotes = getRemotes(structure);

							// short (kv length) :: kv :: key

							final boolean changed = remotes.remove(compose(
									new ShortUnion(short_(source.getPrefix().length())).getBytes(), source.getPrefix(), pointer.key
							));

							if (Distortion.DEBUG) {
								if (!changed) {
									throw new RuntimeException();
								}
							}
						}

						return convert(structure);

					} finally {
						if (do_lock) {
							SerializationUtil.this.set_thread_lock.accept(false);
							this.lock.unlockWrite(write_stamp);
						}
					}
				}
				default:
					throw new IllegalArgumentException();
			}
		}

		/**
		 * Store an object in a {@link KV} at the specified key, drilling into the KV if necessary for Distortion structures
		 * and objects
		 *
		 * @see #convert convert (for metadata spec)
		 *
		 * @param kv The {@link KV} to store the object in
		 * @param key The {@link ByteArray} key to store the object at
		 * @param object The object in question (must be supported by {@link #serialize} or be a Distortion structure/object)
		 * @return The resulting {@link DistortionObject} that represents the stored object, if compatible
		 */
		public DStructure store(final KV<BA> kv, final ByteArray key, final Object object) {
			return store(kv, key, null, object, true);
		}

		/**
		 * <p>Store an object in a {@link KV} at the specified key, drilling into the KV if necessary for Distortion
		 * structures and objects</p>
		 *
		 * <p>If objPrefix is not null, all indexes will be prefixed with it</p>
		 *
		 * @see #convert convert (for metadata spec)
		 *
		 * @param kv The {@link KV} to store the object in
		 * @param key The {@link ByteArray} key to store the object in
		 * @param objPrefix The optional {@link ByteArray} prefix to be prepended to the serialized object
		 * @param obj The object in question (must be supported by {@link #serialize} or be a Distortion structure/object)
		 * @return The resulting {@link DistortionObject} that represents the stored object, if compatible
		 */
		public DStructure store(
				final KV<BA> kv, final ByteArray key, final @Nullable ByteArray objPrefix, final Object obj
		) {
			return store(kv, key, objPrefix, obj, true);
		}

		private DStructure store(
				final KV<BA> kv, final ByteArray key, final @Nullable ByteArray objPrefix, final Object obj,
				final boolean add_to_live
		) {
			final boolean do_lock = !SerializationUtil.this.get_thread_lock.getAsBoolean();
			final KV structure_kv = kv.drill(compose((byte) 0x70, key));

			final @Nullable ByteArray read = kv.read(key);

			if (
					obj instanceof Struct || obj instanceof DistortionStructObject || obj instanceof StructSpec
							|| obj instanceof List || obj instanceof DistortionListObject || obj instanceof ListSpec
							|| obj instanceof Map || obj instanceof DistortionMapObject || obj instanceof MapSpec
							|| obj instanceof Set || obj instanceof DistortionSetObject || obj instanceof SetSpec
							|| obj instanceof DistortionNavigableMapObject || obj instanceof NavigableMapSpec
							|| obj instanceof DistortionNavigableSetObject || obj instanceof NavigableSetSpec
			) {
				final long write_stamp = do_lock ? this.lock.writeLock() : 0;
				if (do_lock) SerializationUtil.this.set_thread_lock.accept(true);
				try {

					dump(kv, key, structure_kv.getPrefix());

				} finally {
					if (do_lock) {
						SerializationUtil.this.set_thread_lock.accept(false);
						this.lock.unlockWrite(write_stamp);
					}
				}
			}

			if (obj instanceof Struct || obj instanceof DistortionStructObject || obj instanceof StructSpec) {

				final @Nullable Struct tocopy;
				final int size;
				final @Nullable String custom_class;

				Object unsafe_dump = null;

				if (obj instanceof Struct && !(obj instanceof DStruct)) {
					tocopy = (Struct) obj;
					size = tocopy.size();
					custom_class = null;
				} else if (
						obj instanceof DistortionStructObject
								&& !((unsafe_dump = unsafe.getObject(obj, struct_field_offset)) instanceof DStruct)
				) {
					tocopy = (Struct) unsafe_dump;
					size = tocopy.size();
					custom_class = 'x' + obj.getClass().getName();
				} else if (obj instanceof StructSpec) {
					tocopy = null;
					size = ((StructSpec) obj).size;
					custom_class = ((StructSpec) obj).custom_class;
				} else {
					if (obj instanceof Struct) {
						return reinsert(kv, key, objPrefix, (DStructure) obj);
					} else {
						return reinsert(kv, key, objPrefix, (DStructure) unsafe_dump);
					}
				}

				final DStruct newstruct = new DStruct(structure_kv, key, SerializationUtil.this, this, size);

				if (tocopy != null) {
					for (int i = 0; i < tocopy.size(); i++) {
						newstruct.set(i, tocopy.get(i));
					}
				}

				if (custom_class != null) {
					newstruct.setCustomClass(custom_class);
				}

				if (objPrefix == null) {
					kv.write(key, compose((byte) 0x71, key));
				} else {
					kv.write(key, compose(objPrefix, (byte) 0x71, key));
				}

				if (add_to_live) {
					final long write_stamp = do_lock ? this.lock.writeLock() : 0;
					try {

						this.live_structures.put(structure_kv.getPrefix(), new WeakReference<>(newstruct));

					} finally {
						if (do_lock) {
							this.lock.unlockWrite(write_stamp);
						}
					}
				}

				return newstruct;

			} else if (obj instanceof List || obj instanceof DistortionListObject || obj instanceof ListSpec) {

				final List tocopy;
				final @Nullable String custom_class;

				Object unsafe_dump = null;

				if (obj instanceof List && !(obj instanceof DList)) {
					tocopy = (List) obj;
					custom_class = null;
				} else if (
						obj instanceof DistortionListObject
								&& !((unsafe_dump = unsafe.getObject(obj, list_field_offset)) instanceof DList)
				) {
					tocopy = (List) unsafe_dump;
					custom_class = 'l' + obj.getClass().getName();
				} else if (obj instanceof ListSpec) {
					tocopy = Collections.EMPTY_LIST;
					custom_class = ((ListSpec) obj).custom_class;
				} else {
					if (obj instanceof List) {
						return reinsert(kv, key, objPrefix, (DStructure) obj);
					} else {
						return reinsert(kv, key, objPrefix, (DStructure) unsafe_dump);
					}
				}

				final DList newlist = new DList(structure_kv, key, SerializationUtil.this, this, true);
				newlist.addAll(tocopy);

				if (custom_class != null) {
					newlist.setCustomClass(custom_class);
				}

				if (objPrefix == null) {
					kv.write(key, compose((byte) 0x72, key));
				} else {
					kv.write(key, compose(objPrefix, (byte) 0x72, key));
				}

				if (add_to_live) {
					final long write_stamp = do_lock ? this.lock.writeLock() : 0;
					try {

						this.live_structures.put(structure_kv.getPrefix(), new WeakReference<>(newlist));

					} finally {
						if (do_lock) {
							this.lock.unlockWrite(write_stamp);
						}
					}
				}

				return newlist;

			} else if (
					obj instanceof SortedMap || obj instanceof DistortionNavigableMapObject || obj instanceof NavigableMapSpec
			) {
				final SortedMap tocopy;
				final @Nullable String custom_class;
				short concurrencyLevel;
				final @Nullable Comparator comparator;

				Object unsafe_dump = null;

				if (obj instanceof SortedMap && !(obj instanceof DNavigableMap)) {
					tocopy = (SortedMap) obj;
					custom_class = null;
					concurrencyLevel = SerializationUtil.this.defaultConcurrencyLevel;
					comparator = tocopy.comparator();
				} else if (
						obj instanceof DistortionNavigableMapObject
								&& !((unsafe_dump = unsafe.getObject(obj, navigable_map_field_offset)) instanceof DNavigableMap)
				) {
					tocopy = (SortedMap) unsafe_dump;
					custom_class = 'd' + obj.getClass().getName();
					concurrencyLevel = unsafe.getShort(obj, navigable_map_conc_field_offset);
					concurrencyLevel = concurrencyLevel == 0 ? SerializationUtil.this.defaultConcurrencyLevel : concurrencyLevel;
					comparator = tocopy.comparator();
				} else if (obj instanceof NavigableMapSpec) {
					tocopy = Collections.emptyNavigableMap();
					custom_class = ((NavigableMapSpec) obj).custom_class;
					concurrencyLevel = ((NavigableMapSpec) obj).concurrency_level;
					comparator = ((NavigableMapSpec) obj).comparator;
				} else {
					if (obj instanceof SortedMap) {
						return reinsert(kv, key, objPrefix, (DStructure) obj);
					} else {
						return reinsert(kv, key, objPrefix, (DStructure) unsafe_dump);
					}
				}

				final DNavigableMap newmap
						= new DNavigableMap(structure_kv, key, SerializationUtil.this, this, concurrencyLevel, comparator);
				newmap.putAll(tocopy);

				if (custom_class != null) {
					newmap.setCustomClass(custom_class);
				}

				if (objPrefix == null) {
					kv.write(key, compose((byte) 0x75, key));
				} else {
					kv.write(key, compose(objPrefix, (byte) 0x75, key));
				}

				if (add_to_live) {
					final long write_stamp = do_lock ? this.lock.writeLock() : 0;
					try {

						this.live_structures.put(structure_kv.getPrefix(), new WeakReference<>(newmap));

					} finally {
						if (do_lock) {
							this.lock.unlockWrite(write_stamp);
						}
					}
				}

				return newmap;

			} else if (
					obj instanceof SortedSet || obj instanceof DistortionNavigableSetObject || obj instanceof NavigableSetSpec
			) {
				final SortedSet tocopy;
				final @Nullable String custom_class;
				short concurrencyLevel;
				final @Nullable Comparator comparator;

				Object unsafe_dump = null;

				if (obj instanceof SortedSet && !(obj instanceof DNavigableSet)) {
					tocopy = (SortedSet) obj;
					custom_class = null;
					concurrencyLevel = SerializationUtil.this.defaultConcurrencyLevel;
					comparator = tocopy.comparator();
				} else if (
						obj instanceof DistortionNavigableSetObject
								&& !((unsafe_dump = unsafe.getObject(obj, navigable_set_field_offset)) instanceof DNavigableSet)
				) {
					tocopy = (SortedSet) unsafe_dump;
					custom_class = 'i' + obj.getClass().getName();
					concurrencyLevel = unsafe.getShort(obj, navigable_set_conc_field_offset);
					concurrencyLevel = concurrencyLevel == 0 ? SerializationUtil.this.defaultConcurrencyLevel : concurrencyLevel;
					comparator = tocopy.comparator();
				} else if (obj instanceof NavigableSetSpec) {
					tocopy = Collections.emptyNavigableSet();
					custom_class = ((NavigableSetSpec) obj).custom_class;
					concurrencyLevel = ((NavigableSetSpec) obj).concurrency_level;
					comparator = ((NavigableSetSpec) obj).comparator;
				} else {
					if (obj instanceof SortedSet) {
						return reinsert(kv, key, objPrefix, (DStructure) obj);
					} else {
						return reinsert(kv, key, objPrefix, (DStructure) unsafe_dump);
					}
				}

				final DNavigableSet newset = new DNavigableSet(
						new DNavigableMap(structure_kv, key, SerializationUtil.this, this, concurrencyLevel, comparator)
				);
				newset.addAll(tocopy);

				if (custom_class != null) {
					newset.setCustomClass(custom_class);
				}

				if (objPrefix == null) {
					kv.write(key, compose((byte) 0x76, key));
				} else {
					kv.write(key, compose(objPrefix, (byte) 0x76, key));
				}

				if (add_to_live) {
					final long write_stamp = do_lock ? this.lock.writeLock() : 0;
					try {

						this.live_structures.put(structure_kv.getPrefix(), new WeakReference<>(newset));

					} finally {
						if (do_lock) {
							this.lock.unlockWrite(write_stamp);
						}
					}
				}

				return newset;

			} else if (obj instanceof Map || obj instanceof DistortionMapObject || obj instanceof MapSpec) {

				final Map tocopy;
				final @Nullable String custom_class;
				short concurrencyLevel;

				if (obj instanceof Map && !(obj instanceof DMap)) {
					tocopy = (Map) obj;
					custom_class = null;
					concurrencyLevel = SerializationUtil.this.defaultConcurrencyLevel;
				} else if (obj instanceof DistortionMapObject && !(unsafe.getObject(obj, map_field_offset) instanceof DMap)) {
					tocopy = (Map) unsafe.getObject(obj, map_field_offset);
					custom_class = 'm' + obj.getClass().getName();
					concurrencyLevel = unsafe.getShort(obj, map_conc_field_offset);
					concurrencyLevel = concurrencyLevel == 0 ? SerializationUtil.this.defaultConcurrencyLevel : concurrencyLevel;
				} else if (obj instanceof MapSpec) {
					tocopy = Collections.EMPTY_MAP;
					custom_class = ((MapSpec) obj).custom_class;
					concurrencyLevel = ((MapSpec) obj).concurrency_level;
				} else {
					if (obj instanceof Map) {
						return reinsert(kv, key, objPrefix, (DStructure) obj);
					} else {
						return reinsert(kv, key, objPrefix, (DStructure) unsafe.getObject(obj, map_field_offset));
					}
				}

				final DMap newmap = new DMap(structure_kv, key, SerializationUtil.this, this, concurrencyLevel);
				newmap.putAll(tocopy);

				if (custom_class != null) {
					newmap.setCustomClass(custom_class);
				}

				if (objPrefix == null) {
					kv.write(key, compose((byte) 0x73, key));
				} else {
					kv.write(key, compose(objPrefix, (byte) 0x73, key));
				}

				if (add_to_live) {
					final long write_stamp = do_lock ? this.lock.writeLock() : 0;
					try {

						this.live_structures.put(structure_kv.getPrefix(), new WeakReference<>(newmap));

					} finally {
						if (do_lock) {
							this.lock.unlockWrite(write_stamp);
						}
					}
				}

				return newmap;

			} else if (obj instanceof Set || obj instanceof DistortionSetObject || obj instanceof SetSpec) {

				final Set tocopy;
				final @Nullable String custom_class;
				short concurrencyLevel;

				if (obj instanceof Set && !(obj instanceof DSet)) {
					tocopy = (Set) obj;
					custom_class = null;
					concurrencyLevel = SerializationUtil.this.defaultConcurrencyLevel;
				} else if (obj instanceof DistortionSetObject && !(unsafe.getObject(obj, set_field_offset) instanceof DSet)) {
					tocopy = (Set) unsafe.getObject(obj, set_field_offset);
					custom_class = 's' + obj.getClass().getName();
					concurrencyLevel = unsafe.getShort(obj, set_conc_field_offset);
					concurrencyLevel = concurrencyLevel == 0 ? SerializationUtil.this.defaultConcurrencyLevel : concurrencyLevel;
				} else if (obj instanceof SetSpec) {
					tocopy = Collections.EMPTY_SET;
					custom_class = ((SetSpec) obj).custom_class;
					concurrencyLevel = ((SetSpec) obj).concurrency_level;
				} else {
					if (obj instanceof Set) {
						return reinsert(kv, key, objPrefix, (DStructure) obj);
					} else {
						return reinsert(kv, key, objPrefix, (DStructure) unsafe.getObject(obj, set_field_offset));
					}
				}

				final DMap newmap = new DMap(structure_kv, key, SerializationUtil.this, this, concurrencyLevel);
				final DSet newset = new DSet(newmap);
				newset.addAll(tocopy);

				if (custom_class != null) {
					newset.setCustomClass(custom_class);
				}

				if (objPrefix == null) {
					kv.write(key, compose((byte) 0x74, key));
				} else {
					kv.write(key, compose(objPrefix, (byte) 0x74, key));
				}

				if (add_to_live) {
					final long write_stamp = do_lock ? this.lock.writeLock() : 0;
					try {

						this.live_structures.put(structure_kv.getPrefix(), new WeakReference<>(newset));

					} finally {
						if (do_lock) {
							this.lock.unlockWrite(write_stamp);
						}
					}
				}

				return newset;

			} else {

				if (objPrefix == null) {
					//noinspection ConstantConditions
					kv.write(key, serialize(obj));
				} else {
					kv.write(key, compose(objPrefix, serialize(obj)));
				}

				return null;
			}
		}

		private DStructure reinsert(
				final KV<BA> kv, final ByteArray key, final @Nullable ByteArray objPrefix, final DStructure source
		) {
			// sources:
			// - normal ref
			// - tagged ref

			// cases:
			// - 1 - only create pointer - normal to longer
			// - 2 - move object, substitute with pointer - normal to shorter
			// - 3 - move object - tagged to normal

			final boolean do_lock = !SerializationUtil.this.get_thread_lock.getAsBoolean();

			final long write_stamp = do_lock ? this.lock.writeLock() : 0;
			if (Distortion.DEBUG) {
				if (do_lock) SerializationUtil.this.set_thread_lock.accept(true);
			}
			try {
				synchronized (source) {

					final KV<BA> source_kv = source.getKV();
					final int source_kv_len = source_kv.getPrefix().length();

					final ByteArray source_key = source.getKey();
					final short source_key_len = short_(source_key.length());

					final KV<BA> dest_kv = kv.drill(compose((byte) 0x70, key));
					final int dest_kv_len = dest_kv.getPrefix().length();

					final DStructure tagged_ref = this.tagged_structures.remove(source_kv.getPrefix());

					if (tagged_ref != null) {

						// CASE 3

						final DStructure new_structure = store(kv, key, objPrefix, spec(source), false);
						move(source, new_structure);
						source.setKV(dest_kv);
						source.setKey(key);
						this.live_structures.put(dest_kv.getPrefix(), new WeakReference<>(source));

					} else {
						if (dest_kv_len < source_kv_len) {

							// CASE 2

							final Object removed = this.live_structures.remove(source_kv.getPrefix());

							if (Distortion.DEBUG) {
								if (removed == null) {
									throw new RuntimeException();
								}
							}

							final DStructure new_structure = store(kv, key, objPrefix, spec(source), false);
							move(source, new_structure);
							source.setKV(dest_kv);
							source.setKey(key);
							this.live_structures.put(dest_kv.getPrefix(), new WeakReference<>(source));

							// pointer stuff

							final KV<BA> containing_kv = this.root.drill(
									source_kv.getPrefix().slice(0, source_kv.getPrefix().length() - 1 - source_key_len)
							);

							final ByteArray source_array = containing_kv.read(source_key);
							final ByteArray source_prefix = source_array.slice(0, source_array.length() - 1 - source_key_len);

							final ByteArray descriptor = compose(code(source), key);

							final Pointer pointer = new Pointer(source_prefix, kv.getPrefix(), descriptor, source_key);

							containing_kv.write(source_key, pointer.serialize());

							// short (kv length) :: kv :: key

							getOrMakeRemotes(source).add(compose(
									new ShortUnion(short_(containing_kv.getPrefix().length())).getBytes(),
									containing_kv.getPrefix(), source_key
							));

						} else {

							// CASE 1

							final ByteArray descriptor = compose(code(source), source_key);

							final Pointer pointer = new Pointer(
									objPrefix, source_kv.getPrefix().slice(0, source_kv.getPrefix().length() - 1 - source_key_len),
									descriptor, key
							);

							kv.write(key, pointer.serialize());

							// short (kv length) :: kv :: key

							getOrMakeRemotes(source).add(compose(
									new ShortUnion(short_(kv.getPrefix().length())).getBytes(), kv.getPrefix(), key
							));
						}
					}

					return source;
				}

			} finally {
				if (do_lock) {
					if (Distortion.DEBUG) {
						SerializationUtil.this.set_thread_lock.accept(false);
					}
					this.lock.unlockWrite(write_stamp);
				}
			}
		}

		private void dump(final KV<BA> kv, final ByteArray key, final ByteArray address) {

			if (Distortion.DEBUG) {
				if (!SerializationUtil.this.get_thread_lock.getAsBoolean()) {
					throw new RuntimeException();
				}
			}

			final WeakReference<DStructure> weak_ref = this.live_structures.remove(address);

			DStructure structure = weak_ref == null ? null : weak_ref.get();

			if (structure == null) {
				structure = this.tagged_structures.remove(address);
			}

			if (structure == null) {
				return;
			}

			while (true) {

				final long random = ThreadLocalRandom.current().nextLong();
				final ByteArray random_bytes = new LongUnion(random).toBA();

				final KV<BA> drilled = this.root.drill(compose(FF, random_bytes));

				if (this.tagged_structures.containsKey(drilled.getPrefix())) {
					continue;
				}

				final DStructure dump;
				if (structure instanceof DStruct) {

					dump = new DStruct(drilled, random_bytes, SerializationUtil.this, this, ((DStruct) structure).size());

				} else if (structure instanceof DList) {

					dump = new DList(drilled, random_bytes, SerializationUtil.this, this, true);

				} else if (structure instanceof DMap) {

					dump = new DMap(
							drilled, random_bytes, SerializationUtil.this, this, ((DMap) structure).getConcurrencyLevel()
					);

				} else if (structure instanceof DSet) {

					final DMap set_map = new DMap(
							drilled, random_bytes, SerializationUtil.this, this, ((DSet) structure).getConcurrencyLevel()
					);
					dump = new DSet(set_map);

				} else if (structure instanceof DNavigableMap) {

					dump = new DNavigableMap(
							drilled, random_bytes, SerializationUtil.this, this, ((DNavigableMap) structure).getConcurrencyLevel(),
							((DNavigableMap) structure).comparator()
					);

				} else if (structure instanceof DNavigableSet) {

					dump = new DNavigableSet(new DNavigableMap(
							drilled, random_bytes, SerializationUtil.this, this, ((DNavigableSet) structure).getConcurrencyLevel(),
							((DNavigableSet) structure).comparator()
					));

				} else {
					throw new RuntimeException();
				}

				synchronized (structure) {

					move(structure, dump);

					structure.setKV(drilled);
					structure.setKey(random_bytes);

					this.tagged_structures.put(drilled.getPrefix(), structure);
				}
				return;
			}
		}

		private void move(final DStructure from, final DStructure to) {

			if (Distortion.DEBUG) {
				if (!SerializationUtil.this.get_thread_lock.getAsBoolean()) {
					throw new RuntimeException();
				}

				if (!Thread.holdsLock(from)) {
					throw new RuntimeException();
				}
			}

			this.tag_pointed++;

			if (from instanceof DStruct) {
				final DStruct from_struct = (DStruct) from;
				final DStruct to_struct = (DStruct) to;

				for (int i = 0; i < from_struct.size(); i++) {
					final Object element = strip(from_struct.get(i));

					if (element instanceof DStructure) {
						final DStructure ds_element = (DStructure) element;
						final @Nullable ByteArray forwarded_fetch = this.forwarded.get(ds_element);

						synchronized (ds_element) {
							if (forwarded_fetch != null) {

								final Set<ByteArray> remotes = getRemotes(ds_element);
								final boolean changed = remotes.remove(forwarded_fetch);

								if (Distortion.DEBUG) {
									if (!changed) {
										throw new RuntimeException();
									}
								}

								to_struct.set(i, ds_element);

							} else {
								final DStructure new_element = to_struct.reflectingSet(i, spec(ds_element));

								move(ds_element, new_element);

								ds_element.setKV(new_element.getKV());
								ds_element.setKey(new_element.getKey());
							}
						}
					} else {
						to_struct.set(i, element);
					}

					from_struct.set(i, null);
				}

			} else if (from instanceof DCollectionStructure) {
				final DCollectionStructure from_col = (DCollectionStructure) from;
				final DCollectionStructure to_col = (DCollectionStructure) to;

				for (final Iterator it = from_col.iterator(); it.hasNext();) {
					final Object element = strip(it.next());

					if (element instanceof DStructure) {
						final DStructure ds_element = (DStructure) element;
						final @Nullable ByteArray forwarded_fetch = this.forwarded.get(ds_element);

						synchronized (ds_element) {
							if (forwarded_fetch != null) {

								final Set<ByteArray> remotes = getRemotes(ds_element);
								final boolean changed = remotes.remove(forwarded_fetch);

								if (Distortion.DEBUG) {
									if (!changed) {
										throw new RuntimeException();
									}
								}

								to_col.add(element);

							} else {
								final DStructure new_element = to_col.reflectingAdd(spec(ds_element));

								move(ds_element, new_element);

								ds_element.setKV(new_element.getKV());
								ds_element.setKey(new_element.getKey());
							}
						}
					} else {
						to_col.add(element);
					}

					if (Distortion.DEBUG) {
						if (this.no_unpack) {
							throw new RuntimeException();
						}
					}

					this.no_unpack = true;
					it.remove();
					this.no_unpack = false;
				}

			} else {
				final DMapStructure from_map = (DMapStructure) from;
				final DMapStructure to_map = (DMapStructure) to;

				for (final Iterator<Entry<Object, Object>> it = from_map.entrySet().iterator(); it.hasNext();) {
					final Entry<Object, Object> entry = it.next();

					final Object key = strip(entry.getKey());
					final Object value = strip(entry.getValue());

					if (key instanceof DStructure && value instanceof DStructure) {

						final DStructure ds_key = (DStructure) key;
						final DStructure ds_value = (DStructure) value;

						final @Nullable ByteArray forwarded_key_fetch = this.forwarded.get(ds_key);
						final @Nullable ByteArray forwarded_value_fetch = this.forwarded.get(ds_value);

						if (forwarded_key_fetch != null && forwarded_value_fetch != null) {

							synchronized (ds_key) {
								synchronized (ds_value) {
									final Set<ByteArray> key_remotes = getRemotes(ds_key);
									final boolean key_changed = key_remotes.remove(forwarded_key_fetch);

									final Set<ByteArray> value_remotes = getRemotes(ds_value);
									final boolean value_changed = value_remotes.remove(forwarded_value_fetch);

									if (Distortion.DEBUG) {
										if (!key_changed || !value_changed) {
											throw new RuntimeException();
										}
									}
								}
							}

							to_map.put(ds_key, ds_value);

						} else if (forwarded_key_fetch == null && forwarded_value_fetch == null) {

							final PutReflection put = to_map.reflectingPut(spec(ds_key), spec(ds_value));

							synchronized (ds_key) {
								move(ds_key, put.key_reflection);

								ds_key.setKV(put.key_reflection.getKV());
								ds_key.setKey(put.key_reflection.getKey());
							}

							synchronized (ds_value) {
								move(ds_value, put.value_reflection);

								ds_value.setKV(put.value_reflection.getKV());
								ds_value.setKey(put.key_reflection.getKey());
							}

						} else if (forwarded_key_fetch != null) {

							synchronized (ds_key) {
								final Set<ByteArray> key_remotes = getRemotes(ds_key);
								final boolean key_changed = key_remotes.remove(forwarded_key_fetch);

								if (Distortion.DEBUG) {
									if (!key_changed) {
										throw new RuntimeException();
									}
								}
							}

							final PutReflection put = to_map.reflectingPut(key, spec(ds_value));

							synchronized (ds_value) {
								move(ds_value, put.value_reflection);

								ds_value.setKV(put.value_reflection.getKV());
								ds_value.setKey(put.value_reflection.getKey());
							}

						} else {

							synchronized (ds_value) {
								final Set<ByteArray> value_remotes = getRemotes(ds_value);
								final boolean value_changed = value_remotes.remove(forwarded_value_fetch);

								if (Distortion.DEBUG) {
									if (!value_changed) {
										throw new RuntimeException();
									}
								}
							}

							final PutReflection put = to_map.reflectingPut(spec(ds_key), value);

							synchronized (ds_key) {
								move(ds_key, put.key_reflection);

								ds_key.setKV(put.key_reflection.getKV());
								ds_key.setKey(put.key_reflection.getKey());
							}
						}
					} else if (key instanceof DStructure) {

						final DStructure ds_key = (DStructure) key;
						final @Nullable ByteArray forwarded_key_fetch = this.forwarded.get(ds_key);

						if (forwarded_key_fetch != null) {

							synchronized (ds_key) {
								final Set<ByteArray> key_remotes = getRemotes(ds_key);
								final boolean key_changed = key_remotes.remove(forwarded_key_fetch);

								if (Distortion.DEBUG) {
									if (!key_changed) {
										throw new RuntimeException();
									}
								}
							}

							to_map.put(ds_key, value);

						} else {

							final PutReflection put = to_map.reflectingPut(spec(ds_key), value);

							synchronized (ds_key) {
								move(ds_key, put.key_reflection);

								ds_key.setKV(put.key_reflection.getKV());
								ds_key.setKey(put.key_reflection.getKey());
							}
						}
					} else if (value instanceof DStructure) {

						final DStructure ds_value = (DStructure) value;
						final @Nullable ByteArray forwarded_value_fetch = this.forwarded.get(ds_value);

						if (forwarded_value_fetch != null) {

							synchronized (ds_value) {
								final Set<ByteArray> value_remotes = getRemotes(ds_value);
								final boolean value_changed = value_remotes.remove(forwarded_value_fetch);

								if (Distortion.DEBUG) {
									if (!value_changed) {
										throw new RuntimeException();
									}
								}
							}

							to_map.put(key, ds_value);

						} else {

							final PutReflection put = to_map.reflectingPut(key, spec(ds_value));

							synchronized (ds_value) {
								move(ds_value, put.value_reflection);

								ds_value.setKV(put.value_reflection.getKV());
								ds_value.setKey(put.value_reflection.getKey());
							}
						}
					} else {
						to_map.put(key, value);
					}

					if (Distortion.DEBUG) {
						if (this.no_unpack) {
							throw new RuntimeException();
						}
					}

					this.no_unpack = true;
					it.remove();
					this.no_unpack = false;
				}
			}

			final String custom_class = from.getCustomClass();

			if (custom_class != null) {
				to.setCustomClass(custom_class);
			}

			// short (kv length) :: kv :: key

			final Set<ByteArray> from_remotes = getRemotes(from);

			final ByteArray to_key = to.getKey();
			final ByteArray to_par_kv_ba
					= to.getKV().getPrefix().slice(0, to.getKV().getPrefix().length() - 1 - to_key.length());

			final BA to_ptr = compose(new ShortUnion(short_(to_par_kv_ba.length())).getBytes(), to_par_kv_ba, to_key);

			if (Distortion.DEBUG) {
				if (from_remotes != null && from_remotes.size() == 1) {
					final ByteArray remote = from_remotes.iterator().next();

					if (!to_ptr.equals(remote)) {
						throw new RuntimeException("Illegal move");
					}
				}
			}

			if (from_remotes != null && from_remotes.size() > 1) {

				final ByteArray to_kv = to.getKV().getPrefix()
						.slice(0, to.getKV().getPrefix().length() - 1 - to.getKey().length());

				final Set<ByteArray> to_remotes;

				if (Distortion.DEBUG) {
					synchronized (to) {
						to_remotes = getOrMakeRemotes(to);
					}
				} else {
					to_remotes = getOrMakeRemotes(to);
				}

				boolean removed_flag = false;

				for (final ByteArray remote : from_remotes) {
					if (to_ptr.equals(remote)) {
						removed_flag = true;
						continue;
					}

					final int remote_kv_len = new ShortUnion(remote.read(0), remote.read(1)).getShort();
					final ByteArray remote_kv = remote.slice(2, 2 + remote_kv_len);
					final ByteArray remote_key = remote.slice(2 + remote_kv_len, remote.length());

					final KV<BA> drilled = this.root.drill(remote_kv);

					final Pointer pointer = new Pointer(drilled.read(remote_key), true);

					pointer.kv = to_kv;
					pointer.descriptor = compose(pointer.descriptor.read(0), to_key);

					drilled.write(remote_key, pointer.serialize());

					to_remotes.add(remote);
				}

				if (Distortion.DEBUG) {
					if (!removed_flag) {
						throw new RuntimeException("Illegal move");
					}
				}
			}

			from.destroy();

			if (--this.tag_pointed == 0) {
				this.forwarded.clear();
			}
		}

		private @Nullable Set<ByteArray> getRemotes(final DStructure structure) {

			if (Distortion.DEBUG) {
				if (!Thread.holdsLock(structure)) {
					throw new RuntimeException();
				}
			}

			final ByteArray fetch = structure.getKV().read(structure.getRemotesKey());

			if (fetch == null) {
				return null;
			}

			return (Set<ByteArray>) unpackRemote(structure.getKV(), fetch);
		}

		private Set<ByteArray> getOrMakeRemotes(final DStructure structure) {

			if (Distortion.DEBUG) {
				if (!Thread.holdsLock(structure)) {
					throw new RuntimeException();
				}
			}

			final ByteArray fetch = structure.getKV().read(structure.getRemotesKey());

			if (fetch == null) {
				return (Set<ByteArray>) store(structure.getKV(), structure.getRemotesKey(), null, new HashSet<>(), false);
			}

			return (Set<ByteArray>) unpackRemote(structure.getKV(), fetch);
		}

		private ByteArray remoteWithShortestKV(final Set<ByteArray> remotes) {
			ByteArray toreturn = null;
			short length = Short.MAX_VALUE;

			for (final ByteArray remote : remotes) {

				final short remote_len = new ShortUnion(remote.read(0), remote.read(1)).getShort();

				if (remote_len < length) {
					toreturn = remote;
					length = remote_len;
				}
			}

			return toreturn;
		}

		class Pointer {

			// PREFIX? :: short (kv length) :: short (descriptor length) :: kv :: descriptor :: key :: short (pointer length)

			public @Nullable ByteArray prefix;
			public ByteArray kv;
			public ByteArray descriptor;
			public ByteArray key;

			Pointer(final @Nullable ByteArray prefix, final ByteArray kv, final ByteArray descriptor, final ByteArray key) {
				this.prefix = prefix;
				this.kv = kv;
				this.descriptor = descriptor;
				this.key = key;
			}

			Pointer(final ByteArray array, final boolean raw) {
				final int length = array.length();

				final int pointer_len = new ShortUnion(array.read(length - 2), array.read(length - 1)).getShort();

				if (raw) {
					this.prefix = array.slice(0, array.length() - pointer_len);
				} else {
					this.prefix = null;
				}

				final int offset = raw ? array.length() - pointer_len + 1 : 0;

				final int kv_len = new ShortUnion(array.read(offset), array.read(offset + 1)).getShort();
				final int descriptor_len = new ShortUnion(array.read(offset + 2), array.read(offset + 3)).getShort();

				this.kv = array.slice(offset + 4, offset + 4 + kv_len);
				this.descriptor = array.slice(offset + 4 + kv_len, offset + 4 + kv_len + descriptor_len);
				this.key = array.slice(offset + 4 + kv_len + descriptor_len, length - 2);
			}

			BA serialize() {
				if (this.prefix != null) {
					return compose(
							this.prefix, (byte) 0xF0,
							new ShortUnion(short_(this.kv.length())).getBytes(),
							new ShortUnion(short_(this.descriptor.length())).getBytes(),
							this.kv, this.descriptor, this.key,
							new ShortUnion(short_(7 + this.kv.length() + this.descriptor.length() + this.key.length())).getBytes()
					);
				} else {
					return compose(
							(byte) 0xF0,
							new ShortUnion(short_(this.kv.length())).getBytes(),
							new ShortUnion(short_(this.descriptor.length())).getBytes(),
							this.kv, this.descriptor, this.key,
							new ShortUnion(short_(7 + this.kv.length() + this.descriptor.length() + this.key.length())).getBytes()
					);
				}
			}
		}

		/**
		 * <p>Converts a plain {@link DStructure} to its DistortionXObject equivalent, if applicable</p>
		 *
		 * <p>DStructure's without a corresponding DistortionXObject will be returned as-is</p>
		 *
		 * <h4>Structure metadata specification</h4>
		 *
		 * <p>The first character is the structure-type code</p>
		 *
		 * <p><tt>
		 *   'x' -- struct  <br />
		 *   'l' -- list    <br />
		 *   'm' -- map     <br />
		 *   's' -- set     <br />
		 *   'd' -- nav map <br />
		 *   'i' -- nav set <br />
		 * </tt></p>
		 *
		 * <p>The rest of the string is the class name of the DistortionXObject</p>
		 *
		 * @param structure The {@link DStructure} to be converted
		 * @return The corresponding DistortionXObject, or the unmodified input
		 */
		private Object convert(final DStructure structure) {
			final String custom_class = structure.getCustomClass();

			if (custom_class != null) {

				final char type = custom_class.charAt(0);
				final String classname = custom_class.substring(1);

				final Class<?> clazz;

				try {
					clazz = Class.forName(classname);
				} catch (final ClassNotFoundException ignored) {
					return structure;
				}

				try {
					switch (type) {
						case 'x': {
							if (DistortionStructObject.class.isAssignableFrom(clazz)) {
								final DistortionStructObject toreturn = (DistortionStructObject) unsafe.allocateInstance(clazz);
								unsafe.putObject(toreturn, struct_field_offset, structure);
								toreturn.distortionInit();
								unsafe.storeFence();
								return toreturn;
							} else {
								throw new IllegalStateException(
										"CLass '" + classname + "' does not extend DistortionStructObject, however it was stored as one previously"
								);
							}
						}
						case 'l': {
							if (DistortionListObject.class.isAssignableFrom(clazz)) {
								final DistortionListObject toreturn = (DistortionListObject) unsafe.allocateInstance(clazz);
								unsafe.putObject(toreturn, list_field_offset, structure);
								toreturn.distortionInit();
								unsafe.storeFence();
								return toreturn;
							} else {
								throw new IllegalStateException(
										"Class '" + classname + "' does not extend DistortionListObject, however it was stored as one previously"
								);
							}
						}
						case 'm': {
							if (DistortionMapObject.class.isAssignableFrom(clazz)) {
								final DistortionMapObject toreturn = (DistortionMapObject) unsafe.allocateInstance(clazz);
								unsafe.putObject(toreturn, map_field_offset, structure);
								unsafe.putShort(toreturn, map_conc_field_offset, (short) -1);
								unsafe.putObject(
										toreturn, map_supplier_field_offset, (IntSupplier) ((DMap) structure)::getConcurrencyLevel
								);
								toreturn.distortionInit();
								unsafe.storeFence();

								return toreturn;
							} else {
								throw new IllegalStateException(
										"Class '" + classname + "' does not extend DistortionMapObject, however it was stored as one previously"
								);
							}
						}
						case 's': {
							if (DistortionSetObject.class.isAssignableFrom(clazz)) {
								final DistortionSetObject toreturn = (DistortionSetObject) unsafe.allocateInstance(clazz);
								unsafe.putObject(toreturn, set_field_offset, structure);
								unsafe.putShort(toreturn, set_conc_field_offset, (short) -1);
								unsafe.putObject(
										toreturn, set_supplier_field_offset, (IntSupplier) ((DSet) structure)::getConcurrencyLevel
								);
								toreturn.distortionInit();
								unsafe.storeFence();
								return toreturn;
							} else {
								throw new IllegalStateException(
										"Class '" + classname + "' does not extend DistortionSetObject, however it was stored as one previously"
								);
							}
						}
						case 'd': {
							if (DistortionNavigableMapObject.class.isAssignableFrom(clazz)) {
								final DistortionNavigableMapObject toreturn = (DistortionNavigableMapObject) unsafe.allocateInstance(clazz);
								unsafe.putObject(toreturn, navigable_map_field_offset, structure);
								unsafe.putShort(toreturn, navigable_map_conc_field_offset, (short) -1);
								unsafe.putObject(
										toreturn, navigable_map_supplier_field_offset, (IntSupplier) ((DNavigableMap) structure)::getConcurrencyLevel
								);
								toreturn.distortionInit();
								unsafe.storeFence();
								return toreturn;
							} else {
								throw new IllegalStateException(
										"Class '" + classname + "' does not extend DistortionNavigableMapObject, however it was stored as one previously"
								);
							}
						}
						case 'i': {
							if (DistortionNavigableSetObject.class.isAssignableFrom(clazz)) {
								final DistortionNavigableSetObject toreturn = (DistortionNavigableSetObject) unsafe.allocateInstance(clazz);
								unsafe.putObject(toreturn, navigable_set_field_offset, structure);
								unsafe.putShort(toreturn, navigable_set_conc_field_offset, (short) -1);
								unsafe.putObject(
										toreturn, navigable_set_supplier_field_offset, (IntSupplier) ((DNavigableSet) structure)::getConcurrencyLevel
								);
								toreturn.distortionInit();
								unsafe.storeFence();
								return toreturn;
							} else {
								throw new IllegalStateException(
										"Class '" + classname + "' does not extend DistortionNavigableSetObject, however it was stored as one previously"
								);
							}
						}
						default:
							//noinspection UseOfSystemOutOrSystemErr
							System.err.println("Unrecognized custom class '" + custom_class + '\'');
							return structure;
					}
				} catch (final InstantiationException e) {
					throw new RuntimeException("Could not instantiate object of class '" + custom_class + '\'', e);
				}
			} else {
				return structure;
			}
		}

		private Object strip(final Object o) {
			if (o instanceof DistortionStructObject) {
				return unsafe.getObject(o, struct_field_offset);
			} else if (o instanceof DistortionListObject) {
				return unsafe.getObject(o, list_field_offset);
			} else if (o instanceof DistortionMapObject) {
				return unsafe.getObject(o, map_field_offset);
			} else if (o instanceof DistortionSetObject) {
				return unsafe.getObject(o, set_field_offset);
			} else if (o instanceof DistortionNavigableMapObject) {
				return unsafe.getObject(o, navigable_map_field_offset);
			} else if (o instanceof DistortionNavigableSetObject) {
				return unsafe.getObject(o, navigable_set_field_offset);
			} else {
				return o;
			}
		}

		@Override
		public void close() {
			final long write_stamp = this.lock.writeLock();
			SerializationUtil.this.set_thread_lock.accept(true);
			try {

				while (!this.tagged_structures.isEmpty()) {

					final Iterator<Entry<ByteArray, DStructure>> it = this.tagged_structures.entrySet().iterator();

					final Entry<ByteArray, DStructure> entry = it.next();
					it.remove();

					final DStructure structure = entry.getValue();

					synchronized (structure) {

						@Nullable final Set<ByteArray> remotes = getRemotes(structure);

						if (remotes != null && !remotes.isEmpty()) {

							// short (kv length) :: kv :: key

							final ByteArray new_home = remoteWithShortestKV(remotes);

							final short new_kv_length = new ShortUnion(new_home.read(0), new_home.read(1)).getShort();
							final KV<BA> new_kv = this.root.drill(new_home.slice(2, 2 + new_kv_length));
							final ByteArray new_key = new_home.slice(2 + new_kv_length, new_home.length());

							final Pointer new_home_pointer = new Pointer(new_kv.read(new_key), true);

							final DStructure new_structure = store(new_kv, new_key, new_home_pointer.prefix, spec(structure), false);

							move(structure, new_structure);

							continue;
						}

						kill(entry.getValue());
					}
				}

				this.live_structures.clear();

			} finally {
				SerializationUtil.this.set_thread_lock.accept(false);
				this.lock.unlockWrite(write_stamp);
			}
		}
	}

	public BA javaSerialize(final Serializable object) {
		try (
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				ObjectOutput out = new ObjectOutputStream(baos);
		) {
			out.writeObject(object);
			return BA(baos.toByteArray());
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static Object javaDeserialize(final ByteArray array) {
		try (
				ByteArrayInputStream bais = new ByteArrayInputStream(array.toArray());
				ObjectInput in = new ObjectInputStream(bais);
		) {
			return in.readObject();
		} catch (final IOException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * <p>A utility class to pack smaller primitives into a short, 1-indexed</p>
	 *
	 * <p>Compatible with Java'a {@link DataInputStream} and {@link DataOutputStream} layouts</p>
	 */
	public final class ShortUnion {
		private final short value;

		public ShortUnion(final short v) {
			this.value = v;
		}

		public ShortUnion(final byte b1, final byte b2) {
			this.value = (short) (((b1 & 0xFF) << 8) | (b2 & 0xFF));
		}

		public ShortUnion(final byte[] bytes) {
			this(bytes[0], bytes[1]);
		}

		public ShortUnion(final ByteArray bytes) {
			this(bytes.read(0), bytes.read(1));
		}

		public short getShort() {
			return this.value;
		}

		public byte getByte1() {
			return (byte) (this.value >>> 8);
		}

		public byte getByte2() {
			return (byte) this.value;
		}

		public byte[] getBytes() {
			final byte[] toreturn = new byte[2];

			toreturn[0] = getByte1();
			toreturn[1] = getByte2();

			return toreturn;
		}

		public BA toBA() {
			final BA toreturn = SerializationUtil.this.factory.allocate(2);

			toreturn.write(0, getByte1());
			toreturn.write(1, getByte2());

			return toreturn;
		}
	}

	/**
	 * <p>A utility class to pack smaller primitives into an int, 1-indexed</p>
	 *
	 * <p>Compatible with Java'a {@link DataInputStream} and {@link DataOutputStream} layouts</p>
	 */
	public final class IntUnion {
		private final int value;

		public IntUnion(final int v) {
			this.value = v;
		}

		public IntUnion(final short s1, final short s2) {
			this.value = ((s1 & 0xFFFF) << 16) | (s2 & 0xFFFF);
		}

		public IntUnion(final byte b1, final byte b2, final byte b3, final byte b4) {
			this.value = ((b1 & 0xFF) << 24) | ((b2 & 0xFF) << 16) | ((b3 & 0xFF) << 8) | (b4 & 0xFF);
		}

		public IntUnion(final byte[] bytes) {
			this(bytes[0], bytes[1], bytes[2], bytes[3]);
		}

		public IntUnion(final ByteArray bytes) {
			this(bytes.read(0), bytes.read(1), bytes.read(2), bytes.read(3));
		}

		public int getInt() {
			return this.value;
		}

		public short getShort1() {
			return (short) (this.value >>> 16);
		}

		public short getShort2() {
			return (short) this.value;
		}

		public byte getByte1() {
			return (byte) (this.value >>> 24);
		}

		public byte getByte2() {
			return (byte) (this.value >>> 16);
		}

		public byte getByte3() {
			return (byte) (this.value >>> 8);
		}

		public byte getByte4() {
			return (byte) this.value;
		}

		public byte[] getBytes() {
			final byte[] toreturn = new byte[4];

			toreturn[0] = getByte1();
			toreturn[1] = getByte2();
			toreturn[2] = getByte3();
			toreturn[3] = getByte4();

			return toreturn;
		}

		public BA toBA() {
			final BA toreturn = SerializationUtil.this.factory.allocate(4);

			toreturn.write(0, getByte1());
			toreturn.write(1, getByte2());
			toreturn.write(2, getByte3());
			toreturn.write(3, getByte4());

			return toreturn;
		}
	}

	/**
	 * <p>A utility class to pack smaller primitives into a long, 1-indexed</p>
	 *
	 * <p>Compatible with Java'a {@link DataInputStream} and {@link DataOutputStream} layouts</p>
	 */
	public final class LongUnion {
		private final long value;

		public LongUnion(final long v) {
			this.value = v;
		}

		public LongUnion(final int i1, final int i2) {
			this.value = ((i1 & 0xFFFFFFFFL) << 32) | (i2 & 0xFFFFFFFFL);
		}

		public LongUnion(final short s1, final short s2, final short s3, final short s4) {
			this.value = ((s1 & 0xFFFFL) << 48) | ((s2 & 0xFFFFL) << 32) | ((s3 & 0xFFFFL) << 16) | (s4 & 0xFFFFL);
		}

		public LongUnion(
				final byte b1, final byte b2, final byte b3, final byte b4,
				final byte b5, final byte b6, final byte b7, final byte b8
		) {
			this.value =
					((b1 & 0xFFL) << 56) | ((b2 & 0xFFL) << 48) | ((b3 & 0xFFL) << 40) | ((b4 & 0xFFL) << 32) |
					((b5 & 0xFFL) << 24) | ((b6 & 0xFFL) << 16) | ((b7 & 0xFFL) << 8) | (b8 & 0xFFL);
		}

		public LongUnion(final byte[] bytes) {
			this(bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]);
		}

		public LongUnion(final ByteArray bytes) {
			this(
					bytes.read(0), bytes.read(1), bytes.read(2), bytes.read(3),
					bytes.read(4), bytes.read(5), bytes.read(6), bytes.read(7)
			);
		}

		public long getLong() {
			return this.value;
		}

		public int getInt1() {
			return (int) (this.value >>> 32);
		}

		public int getInt2() {
			return (int) this.value;
		}

		public short getShort1() {
			return (short) (this.value >>> 48);
		}

		public short getShort2() {
			return (short) (this.value >>> 32);
		}

		public short getShort3() {
			return (short) (this.value >>> 16);
		}

		public short getShort4() {
			return (short) this.value;
		}

		public byte getByte1() {
			return (byte) (this.value >>> 56);
		}

		public byte getByte2() {
			return (byte) (this.value >>> 48);
		}

		public byte getByte3() {
			return (byte) (this.value >>> 40);
		}

		public byte getByte4() {
			return (byte) (this.value >>> 32);
		}

		public byte getByte5() {
			return (byte) (this.value >>> 24);
		}

		public byte getByte6() {
			return (byte) (this.value >>> 16);
		}

		public byte getByte7() {
			return (byte) (this.value >>> 8);
		}

		public byte getByte8() {
			return (byte) this.value;
		}

		public byte[] getBytes() {
			final byte[] toreturn = new byte[8];

			toreturn[0] = getByte1();
			toreturn[1] = getByte2();
			toreturn[2] = getByte3();
			toreturn[3] = getByte4();
			toreturn[4] = getByte5();
			toreturn[5] = getByte6();
			toreturn[6] = getByte7();
			toreturn[7] = getByte8();

			return toreturn;
		}

		public BA toBA() {
			final BA toreturn = SerializationUtil.this.factory.allocate(8);

			toreturn.write(0, getByte1());
			toreturn.write(1, getByte2());
			toreturn.write(2, getByte3());
			toreturn.write(3, getByte4());
			toreturn.write(4, getByte5());
			toreturn.write(5, getByte6());
			toreturn.write(6, getByte7());
			toreturn.write(7, getByte8());

			return toreturn;
		}
	}

	public static final Comparator<Boolean> bool_comp = Boolean::compare;
	public static final Comparator<Byte> byte_comp = Byte::compare;
	public static final Comparator<Character> char_comp = Character::compare;
	public static final Comparator<Short> short_comp = Short::compare;
	public static final Comparator<Integer> int_comp = Integer::compare;
	public static final Comparator<Long> long_comp = Long::compare;
	public static final Comparator<Float> float_comp = Float::compare;
	public static final Comparator<Double> double_comp = Double::compare;

	public static final Comparator<boolean[]> boolar_comp = (boolean[] left, boolean[] right) -> {
		for (int i = 0; i < left.length && i < right.length; i++) {
			final int compare = Boolean.compare(left[i], right[i]);

			if (compare != 0)
				return compare;
		}

		return Integer.compare(left.length, right.length);
	};

	public static final Comparator<byte[]> bytear_comp = (byte[] left, byte[] right) -> {
		for (int i = 0; i < left.length && i < right.length; i++) {
			final int compare = Byte.compare(left[i], right[i]);

			if (compare != 0)
				return compare;
		}

		return Integer.compare(left.length, right.length);
	};

	public static final Comparator<char[]> charar_comp = (char[] left, char[] right) -> {
		for (int i = 0; i < left.length && i < right.length; i++) {
			final int compare = Character.compare(left[i], right[i]);

			if (compare != 0)
				return compare;
		}

		return Integer.compare(left.length, right.length);
	};

	public static final Comparator<short[]> shortar_comp = (short[] left, short[] right) -> {
		for (int i = 0; i < left.length && i < right.length; i++) {
			final int compare = Short.compare(left[i], right[i]);

			if (compare != 0)
				return compare;
		}

		return Integer.compare(left.length, right.length);
	};

	public static final Comparator<int[]> intar_comp = (int[] left, int[] right) -> {
		for (int i = 0; i < left.length && i < right.length; i++) {
			final int compare = Integer.compare(left[i], right[i]);

			if (compare != 0)
				return compare;
		}

		return Integer.compare(left.length, right.length);
	};

	public static final Comparator<long[]> longar_comp = (long[] left, long[] right) -> {
		for (int i = 0; i < left.length && i < right.length; i++) {
			final int compare = Long.compare(left[i], right[i]);

			if (compare != 0)
				return compare;
		}

		return Integer.compare(left.length, right.length);
	};

	public static final Comparator<float[]> floatar_comp = (float[] left, float[] right) -> {
		for (int i = 0; i < left.length && i < right.length; i++) {
			final int compare = Float.compare(left[i], right[i]);

			if (compare != 0)
				return compare;
		}

		return Integer.compare(left.length, right.length);
	};

	public static final Comparator<double[]> doublear_comp = (double[] left, double[] right) -> {
		for (int i = 0; i < left.length && i < right.length; i++) {
			final int compare = Double.compare(left[i], right[i]);

			if (compare != 0)
				return compare;
		}

		return Integer.compare(left.length, right.length);
	};

	public static short short_(final int value) {
		if ((short) value != value) {
			throw new ArithmeticException("key length overflow");
		}

		return (short) value;
	}

}
