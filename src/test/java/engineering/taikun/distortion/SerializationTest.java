package engineering.taikun.distortion;

import engineering.taikun.distortion.SerializationTest.DiffValue.DiffType;
import engineering.taikun.distortion.SerializationUtil.SerializationContext;
import engineering.taikun.distortion.api.structures.*;
import engineering.taikun.distortion.serialization.api.ByteArray;
import engineering.taikun.distortion.serialization.api.ByteArrayFactory;
import engineering.taikun.distortion.serialization.imp.ArrayWrapper;
import engineering.taikun.distortion.serialization.imp.ArrayWrapperFactory;
import engineering.taikun.distortion.store.imp.DebugKV;
import engineering.taikun.distortion.structures.api.Struct;
import engineering.taikun.distortion.structures.imp.DMap;
import engineering.taikun.distortion.structures.imp.SimpleStruct;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;

public class SerializationTest {

	static boolean locked;

	static ByteArrayFactory<ArrayWrapper> factory = new ArrayWrapperFactory();
	static SerializationUtil<ArrayWrapper> util = new SerializationUtil<>(
			factory, (short) 1,
			b -> locked = b,
			() -> locked
	);

	static DebugKV kv = new DebugKV();

	@Test
	public static void main() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

		System.out.println("Serialization test");

		SerializationContext ctx = util.new SerializationContext(kv);

		// compose

		byte[] bytes = "ahsdlkf".getBytes(StandardCharsets.UTF_8);
		byte[][] lots_of_bytes = { bytes, bytes, bytes, bytes };

		util.compose(lots_of_bytes);

		// specs / moving

		DMap<String, Object, ArrayWrapper> dmap = new DMap<>(kv, SerializationUtil.EMPTY_ARRAY, util, ctx, (short) 1);

		Struct test_struct = new SimpleStruct(2);
		test_struct.set(0, "354");
		test_struct.set(1, 56);

		List test_list = new ArrayList();
		test_list.add("asdf");
		test_list.add(5335);

		Map test_map = new HashMap();
		test_map.put("353", 63);
		test_map.put(235, "254");

		Set test_set = new HashSet();
		test_set.add("35");
		test_set.add(35);

		NavigableMap<Integer, Object> test_nav_map = new TreeMap<>(
				(Comparator<? super Integer> & Serializable) Integer::compare);
		test_nav_map.put(42, 353);
		test_nav_map.put(8, "kjk");

		NavigableSet<String> test_nav_set = new TreeSet<>((Comparator<? super String> & Serializable) String::compareTo);
		test_nav_set.add("kjl");
		test_nav_set.add("aaa");

		dmap.put("zz", test_struct);
		dmap.put("a", dmap.get("zz"));

		dmap.put("zz", test_list);
		dmap.put("a", dmap.get("zz"));

		dmap.put("zz", test_map);
		dmap.put("a", dmap.get("zz"));

		dmap.put("zz", test_set);
		dmap.put("a", dmap.get("zz"));

		dmap.put("zz", test_nav_map);
		dmap.put("a", dmap.get("zz"));

		dmap.put("zz", test_nav_set);
		dmap.put("a", dmap.get("zz"));

		dmap.clear();

		ctx.close();

		assert_(kv.map.size() == 3);

		// serializing primitives

		{
			Random random = new Random(654883);

			dmap.put("a", null);
			assert_(Objects.equals(dmap.get("a"), null));

			dmap.put("a", true);
			assert_(Objects.equals(dmap.get("a"), true));

			dmap.put("a", false);
			assert_(Objects.equals(dmap.get("a"), false));

			dmap.put("a", (byte) 0x32);
			assert_(Objects.equals(dmap.get("a"), (byte) 0x32));

			byte[] byte_ar1 = new byte[10];
			random.nextBytes(byte_ar1);

			dmap.put("a", byte_ar1);
			assert_(Arrays.equals((byte[]) dmap.get("a"), byte_ar1));

			byte[] byte_ar2 = new byte[500];
			random.nextBytes(byte_ar2);

			dmap.put("a", byte_ar2);
			assert_(Arrays.equals((byte[]) dmap.get("a"), byte_ar2));

			byte[] byte_ar3 = new byte[100_000];
			random.nextBytes(byte_ar3);

			dmap.put("a", byte_ar3);
			assert_(Arrays.equals((byte[]) dmap.get("a"), byte_ar3));

			dmap.put("a", (short) 23523);
			assert_(Objects.equals(dmap.get("a"), (short) 23523));

			short[] short_ar1 = new short[10];
			for (int i = 0; i < 10; i++)
				short_ar1[i] = (short) random.nextInt();

			dmap.put("a", short_ar1);
			assert_(Arrays.equals((short[]) dmap.get("a"), short_ar1));

			short[] short_ar2 = new short[500];
			for (int i = 0; i < 500; i++)
				short_ar2[i] = (short) random.nextInt();

			dmap.put("a", short_ar2);
			assert_(Arrays.equals((short[]) dmap.get("a"), short_ar2));

			short[] short_ar3 = new short[100_000];
			for (int i = 0; i < 100_000; i++)
				short_ar3[i] = (short) random.nextInt();

			dmap.put("a", short_ar3);
			assert_(Arrays.equals((short[]) dmap.get("a"), short_ar3));

			dmap.put("a", 'c');
			assert_(Objects.equals(dmap.get("a"), 'c'));

			char[] char_ar1 = "j4m32n4k2n4".toCharArray();

			dmap.put("a", char_ar1);
			assert_(Arrays.equals((char[]) dmap.get("a"), char_ar1));

			char[] char_ar2 = new char[500];
			for (int i = 0; i < 500; i++)
				char_ar2[i] = (char) random.nextInt();

			dmap.put("a", char_ar2);
			assert_(Arrays.equals((char[]) dmap.get("a"), char_ar2));

			char[] char_ar3 = new char[100_000];
			for (int i = 0; i < 100_000; i++)
				char_ar3[i] = (char) random.nextInt();

			dmap.put("a", char_ar3);
			assert_(Arrays.equals((char[]) dmap.get("a"), char_ar3));

			dmap.put("a", 987987);
			assert_(Objects.equals(dmap.get("a"), 987987));

			int[] int_ar1 = new int[10];
			for (int i = 0; i < 10; i++)
				int_ar1[i] = random.nextInt();

			dmap.put("a", int_ar1);
			assert_(Arrays.equals((int[]) dmap.get("a"), int_ar1));

			int[] int_ar2 = new int[500];
			for (int i = 0; i < 500; i++)
				int_ar2[i] = random.nextInt();

			dmap.put("a", int_ar2);
			assert_(Arrays.equals((int[]) dmap.get("a"), int_ar2));

			int[] int_ar3 = new int[100_000];
			for (int i = 0; i < 100_000; i++)
				int_ar3[i] = random.nextInt();

			dmap.put("a", int_ar3);
			assert_(Arrays.equals((int[]) dmap.get("a"), int_ar3));

			dmap.put("a", 123123L);
			assert_(Objects.equals(dmap.get("a"), 123123L));

			long[] long_ar1 = new long[10];
			for (int i = 0; i < 10; i++)
				long_ar1[i] = random.nextLong();

			dmap.put("a", long_ar1);
			assert_(Arrays.equals((long[]) dmap.get("a"), long_ar1));

			long[] long_ar2 = new long[500];
			for (int i = 0; i < 500; i++)
				long_ar2[i] = random.nextLong();

			dmap.put("a", long_ar2);
			assert_(Arrays.equals((long[]) dmap.get("a"), long_ar2));

			long[] long_ar3 = new long[100_000];
			for (int i = 0; i < 100_000; i++)
				long_ar3[i] = random.nextLong();

			dmap.put("a", long_ar3);
			assert_(Arrays.equals((long[]) dmap.get("a"), long_ar3));

			dmap.put("a", 0.123f);
			assert_(Objects.equals(dmap.get("a"), 0.123f));

			float[] float_ar1 = new float[10];
			for (int i = 0; i < 10; i++)
				float_ar1[i] = random.nextFloat();

			dmap.put("a", float_ar1);
			assert_(Arrays.equals((float[]) dmap.get("a"), float_ar1));

			float[] float_ar2 = new float[500];
			for (int i = 0; i < 500; i++)
				float_ar2[i] = random.nextFloat();

			dmap.put("a", float_ar2);
			assert_(Arrays.equals((float[]) dmap.get("a"), float_ar2));

			float[] float_ar3 = new float[100_000];
			for (int i = 0; i < 100_000; i++)
				float_ar3[i] = random.nextFloat();

			dmap.put("a", float_ar3);
			assert_(Arrays.equals((float[]) dmap.get("a"), float_ar3));

			dmap.put("a", 123.456);
			assert_(Objects.equals(dmap.get("a"), 123.456));

			double[] double_ar1 = new double[10];
			for (int i = 0; i < 10; i++)
				double_ar1[i] = random.nextDouble();

			dmap.put("a", double_ar1);
			assert_(Arrays.equals((double[]) dmap.get("a"), double_ar1));

			double[] double_ar2 = new double[500];
			for (int i = 0; i < 500; i++)
				double_ar2[i] = random.nextDouble();

			dmap.put("a", double_ar2);
			assert_(Arrays.equals((double[]) dmap.get("a"), double_ar2));

			double[] double_ar3 = new double[100_000];
			for (int i = 0; i < 100_000; i++)
				double_ar3[i] = random.nextDouble();

			dmap.put("a", double_ar3);
			assert_(Arrays.equals((double[]) dmap.get("a"), double_ar3));

			dmap.put("a", "this is a normal string");
			assert_(Objects.equals(dmap.get("a"), "this is a normal string"));

			char[] med_string_chars = new char[500];
			for (int i = 0; i < 500; i++)
				med_string_chars[i] = (char) (32 + random.nextInt(64));
			String med_string = new String(med_string_chars);

			dmap.put("a", med_string);
			assert_(Objects.equals(dmap.get("a"), med_string));

			char[] long_string_chars = new char[100_000];
			for (int i = 0; i < 100_000; i++)
				long_string_chars[i] = (char) (32 + random.nextInt(64));
			String long_string = new String(long_string_chars);

			dmap.put("a", long_string);
			assert_(Objects.equals(dmap.get("a"), long_string));

			String[] string_string1 = {
					"this is a normal string",
					"this is a normal string",
					"this is a normal string",
			};

			dmap.put("a", string_string1);
			assert_(Arrays.equals((String[]) dmap.get("a"), string_string1));

			String[] string_string2 = { med_string, med_string, med_string };

			dmap.put("a", string_string2);
			assert_(Arrays.equals((String[]) dmap.get("a"), string_string2));

			String[] string_string3 = { long_string, long_string, long_string };

			dmap.put("a", string_string3);
			assert_(Arrays.equals((String[]) dmap.get("a"), string_string3));

			dmap.clear();

			ctx.close();

			assert_(kv.map.size() == 3);
		}

		// cold unpack

		dmap.put("a", test_struct);
		dmap.put("b", test_list);
		dmap.put("c", test_map);
		dmap.put("d", test_set);
		dmap.put("e", test_nav_map);
		dmap.put("f", test_nav_set);

		ctx.close();

		Struct struct_fetch = (Struct) dmap.get("a");
		assert_(Objects.equals(struct_fetch.get(0), test_struct.get(0)));
		assert_(Objects.equals(struct_fetch.get(1), test_struct.get(1)));
		assert_(Objects.equals(struct_fetch.size(), test_struct.size()));

		assert_(Objects.equals(dmap.get("b"), test_list));
		assert_(Objects.equals(dmap.get("c"), test_map));
		assert_(Objects.equals(dmap.get("d"), test_set));
		assert_(Objects.equals(dmap.get("e"), test_nav_map));
		assert_(Objects.equals(dmap.get("f"), test_nav_set));

		// blind unpackAndDestroy

		ctx.close();

		dmap.put("a", Collections.EMPTY_LIST);
		dmap.put("b", Collections.EMPTY_LIST);
		dmap.put("c", Collections.EMPTY_LIST);
		dmap.put("d", Collections.EMPTY_LIST);
		dmap.put("e", Collections.EMPTY_LIST);
		dmap.put("f", Collections.EMPTY_LIST);

		dmap.clear();

		ctx.close();

		assert_(kv.map.size() == 3);

		// probe

		dmap.put("a", test_struct);
		dmap.put("az", dmap.get("a"));

		dmap.put("b", test_list);
		dmap.put("bz", dmap.get("b"));

		dmap.put("c", test_map);
		dmap.put("cz", dmap.get("c"));

		dmap.put("d", test_set);
		dmap.put("dz", dmap.get("d"));

		dmap.put("e", test_nav_map);
		dmap.put("ez", dmap.get("e"));

		dmap.put("f", test_nav_set);
		dmap.put("fz", dmap.get("f"));

		ctx.close();

		dmap.put("az", Collections.EMPTY_LIST);
		dmap.put("bz", Collections.EMPTY_LIST);
		dmap.put("cz", Collections.EMPTY_LIST);
		dmap.put("dz", Collections.EMPTY_LIST);
		dmap.put("ez", Collections.EMPTY_LIST);
		dmap.put("fz", Collections.EMPTY_LIST);

		dmap.clear();

		ctx.close();

		assert_(kv.map.size() == 3);

		// DistortionObject shit

		dmap.put("a", new StructTest());
		dmap.put("az", dmap.get("a"));

		dmap.put("b", new ListTest());
		dmap.put("bz", dmap.get("b"));

		dmap.put("c", new MapTest());
		dmap.put("cz", dmap.get("c"));

		dmap.put("d", new SetTest());
		dmap.put("dz", dmap.get("d"));

		dmap.put("e", new NavMapTest());
		dmap.put("ez", dmap.get("e"));

		dmap.put("f", new NavSetTest());
		dmap.put("fz", dmap.get("f"));

		Method struct_get = DistortionStructObject.class.getDeclaredMethod("struct");
		struct_get.setAccessible(true);
		Method list_get = DistortionListObject.class.getDeclaredMethod("list");
		list_get.setAccessible(true);
		Method map_get = DistortionMapObject.class.getDeclaredMethod("map");
		map_get.setAccessible(true);
		Method set_get = DistortionSetObject.class.getDeclaredMethod("set");
		set_get.setAccessible(true);
		Method nav_map_get = DistortionNavigableMapObject.class.getDeclaredMethod("map");
		nav_map_get.setAccessible(true);
		Method nav_set_get = DistortionNavigableSetObject.class.getDeclaredMethod("set");
		nav_set_get.setAccessible(true);

		assert_(struct_get.invoke(dmap.get("a")).equals(stupid_struct));
		assert_(list_get.invoke(dmap.get("b")).equals(stupid_list));
		assert_(map_get.invoke(dmap.get("c")).equals(stupid_map));
		assert_(set_get.invoke(dmap.get("d")).equals(stupid_set));
		assert_(nav_map_get.invoke(dmap.get("e")).equals(stupid_nav_map));
		assert_(nav_set_get.invoke(dmap.get("f")).equals(stupid_nav_set));
//
		assert_(struct_get.invoke(dmap.get("az")).equals(stupid_struct));
		assert_(list_get.invoke(dmap.get("bz")).equals(stupid_list));
		assert_(map_get.invoke(dmap.get("cz")).equals(stupid_map));
		assert_(set_get.invoke(dmap.get("dz")).equals(stupid_set));
		assert_(nav_map_get.invoke(dmap.get("ez")).equals(stupid_nav_map));
		assert_(nav_set_get.invoke(dmap.get("fz")).equals(stupid_nav_set));

		dmap.clear();

		ctx.close();

		assert_(kv.map.size() == 3);

		// non-prefixed DO shit

		dmap.put("a", new ArrayList());

		List list = (List) dmap.get("a");

		list.add(new StructTest());
		list.add(new ListTest());
		list.add(new MapTest());
		list.add(new SetTest());
		list.add(new NavMapTest());
		list.add(new NavSetTest());

		assert_(struct_get.invoke(list.get(0)).equals(stupid_struct));
		assert_(list_get.invoke(list.get(1)).equals(stupid_list));
		assert_(map_get.invoke(list.get(2)).equals(stupid_map));
		assert_(set_get.invoke(list.get(3)).equals(stupid_set));
		assert_(nav_map_get.invoke(list.get(4)).equals(stupid_nav_map));
		assert_(nav_set_get.invoke(list.get(5)).equals(stupid_nav_set));

		dmap.clear();

		ctx.close();

		assert_(kv.map.size() == 3);

		// moving pointer containing structures

		dmap.put("xxx", Collections.singletonList("under the bed"));
		dmap.put("420", Collections.singletonList("just holding it for a friend"));

		Struct naughty_struct = new SimpleStruct(1);
		naughty_struct.set(0, dmap.get("xxx"));

		dmap.put("a", naughty_struct);
		dmap.put("az", dmap.get("a"));

		Struct naughty_struct2 = new SimpleStruct(1);
		naughty_struct2.set(0, Collections.singletonMap("xxx", "moonshine"));

		dmap.put("b", naughty_struct2);
		dmap.put("bz", dmap.get("b"));

		List naughty_list = new ArrayList();
		naughty_list.add(dmap.get("420"));

		dmap.put("c", naughty_list);
		dmap.put("cz", dmap.get("c"));

		List naughty_list2 = new ArrayList();
		naughty_list2.add(Collections.singletonMap(422, "earth day is better anyways"));

		dmap.put("d", naughty_list2);
		dmap.put("dz", dmap.get("d"));

		NavigableMap<List<String>, List<String>> naughty_map1 = new TreeMap<>(
				(Comparator<List<String>> & Serializable) (m1, m2) -> m1.get(0).compareTo(m2.get(0))
		);
		naughty_map1.put(Collections.singletonList("asdf"), Collections.singletonList(";lkj"));

		dmap.put("e", naughty_map1);
		dmap.put("ez", dmap.get("e"));

		NavigableMap<List<String>, List<String>> naughty_map2 = new TreeMap<>(
				(Comparator<List<String>> & Serializable) (m1, m2) -> m1.get(0).compareTo(m2.get(0))
		);
		naughty_map2.put((List<String>) dmap.get("xxx"), (List<String>) dmap.get("420"));

		dmap.put("f", naughty_map2);
		dmap.put("fz", dmap.get("f"));

		NavigableMap<List<String>, List<String>> naughty_map3 = new TreeMap<>(
				(Comparator<List<String>> & Serializable) (m1, m2) -> m1.get(0).compareTo(m2.get(0))
		);
		naughty_map3.put(Collections.singletonList("zzz"), (List<String>) dmap.get("xxx"));

		dmap.put("g", naughty_map3);
		dmap.put("gz", dmap.get("g"));

		NavigableMap<List<String>, List<String>> naughty_map4 = new TreeMap<>(
				(Comparator<List<String>> & Serializable) (m1, m2) -> m1.get(0).compareTo(m2.get(0))
		);
		naughty_map4.put((List<String>) dmap.get("420"), Collections.singletonList("1337"));

		dmap.put("h", naughty_map4);
		dmap.put("hz", dmap.get("h"));

		NavigableMap<List<String>, String> naughty_map5 = new TreeMap<>(
				(Comparator<List<String>> & Serializable) (m1, m2) -> m1.get(0).compareTo(m2.get(0))
		);
		naughty_map5.put(Collections.singletonList("bagged"), "loose");

		dmap.put("i", naughty_map5);
		dmap.put("iz", dmap.get("i"));

		NavigableMap<List<String>, String> naughty_map6 = new TreeMap<>(
				(Comparator<List<String>> & Serializable) (m1, m2) -> m1.get(0).compareTo(m2.get(0))
		);
		naughty_map6.put((List<String>) dmap.get("xxx"), "-> <- -> <-");

		dmap.put("j", naughty_map6);
		dmap.put("jz", dmap.get("j"));

		NavigableMap<String, List<String>> naughty_map7 = new TreeMap<>();
		naughty_map7.put("sock drawer", (List<String>) dmap.get("420"));

		dmap.put("k", naughty_map7);
		dmap.put("kz", dmap.get("k"));

		NavigableMap<String, List<String>> naughty_map8 = new TreeMap<>();
		naughty_map8.put("spice rack", Collections.singletonList("oregano"));

		dmap.put("l", naughty_map8);
		dmap.put("lz", dmap.get("l"));

		dmap.remove("a");
		dmap.remove("b");
		dmap.remove("c");
		dmap.remove("d");
		dmap.remove("e");
		dmap.remove("f");
		dmap.remove("g");
		dmap.remove("h");
		dmap.remove("i");
		dmap.remove("j");
		dmap.remove("k");
		dmap.remove("l");

		ctx.close();

		dmap.clear();

		ctx.close();

		assert_(kv.map.size() == 3);

		// many remotes moving

		dmap.put("rome", Collections.singletonList("capital of the world"));

		dmap.put("los angeles", dmap.get("rome"));
		dmap.put("san francisco", dmap.get("rome"));
		dmap.put("new york", dmap.get("rome"));
		dmap.put("london", dmap.get("rome"));
		dmap.put("athens", dmap.get("rome"));

		ctx.close();

		dmap.remove("rome");

		ctx.close();

		dmap.clear();

		ctx.close();

		assert_(kv.map.size() == 3);

		System.out.println("passed");
	}

	public static void assert_(final boolean bool) {
		if (!bool)
			throw new RuntimeException();
	}

	public static void title(final String title) {
		System.out.println(title);
		System.out.println("-----");
	}

	public static Map<ByteArray, ByteArray> old_data = new HashMap<>();

	public static void diff() {
		final SortedMap<ByteArray, DiffValue> diff = new TreeMap<>();

		for (Entry<ByteArray, ? extends ByteArray> entry : kv.map.entrySet()) {
			ByteArray old_fetch = old_data.get(entry.getKey());

			if (old_fetch == null) {
				diff.put(entry.getKey(), new DiffValue(DiffType.INSERTTION, entry.getValue()));
			} else if (!old_fetch.equals(entry.getValue())) {
				diff.put(entry.getKey(), new DiffValue(DiffType.MUTATION, entry.getValue()));
			}
		}

		for (Entry<ByteArray, ByteArray> entry : old_data.entrySet()) {
			ByteArray new_fetch = kv.map.get(entry.getKey());

			if (new_fetch == null) {
				diff.put(entry.getKey(), new DiffValue(DiffType.REMOVAL, entry.getValue()));
			}
		}

		System.out.println("-----");

		for (Entry<ByteArray, DiffValue> entry : diff.entrySet()) {
			if (entry.getValue().type == DiffType.INSERTTION) {
				System.out.println("+ " + entry.getKey() + " -> " + entry.getValue().data);
			} else if (entry.getValue().type == DiffType.MUTATION) {
				System.out.println("~ " + entry.getKey() + " -> " + entry.getValue().data);
			} else {
				System.out.println("- " + entry.getKey() + " -> " + entry.getValue().data);
			}
		}

		System.out.println("-----");

		old_data.clear();
		old_data.putAll(kv.map);
	}

	public static class DiffValue {
		public enum DiffType {
			INSERTTION, REMOVAL, MUTATION
		}

		public final DiffType type;
		public final ByteArray data;

		public DiffValue(final DiffType type, final ByteArray data) {
			this.type = type;
			this.data = data;
		}
	}

	public static Struct stupid_struct = new SimpleStruct(3);

	static {
		stupid_struct.set(0, Collections.EMPTY_MAP);
		stupid_struct.set(1, "some text");
		stupid_struct.set(2, 123L);
	}

	public static class StructTest extends DistortionStructObject {

		public StructTest() {
			super(new SimpleStruct(3));

			Struct struct = struct();

			struct.set(0, Collections.EMPTY_MAP);
			struct.set(1, "some text");
			struct.set(2, 123L);
		}

		@Override public Struct struct() {
			return super.struct();
		}

		@Override public String toString() {
			return Arrays.asList(struct().get(0), struct().get(1), struct().get(2)).toString();
		}
	}

	public static List stupid_list = new ArrayList();

	static {
		stupid_list.add(Collections.emptyNavigableMap());
		stupid_list.add("879");
		stupid_list.add((byte) 0x34);
	}

	public static class ListTest extends DistortionListObject {

		public ListTest() {
			super(new ArrayList());

			List list = list();

			list.add(Collections.emptyNavigableMap());
			list.add("879");
			list.add((byte) 0x34);
		}

		@Override public List list() {
			return super.list();
		}
	}

	public static Map stupid_map = new HashMap();

	static {
		stupid_map.put(5, "nk");
		stupid_map.put("12", 5);
		stupid_map.put('c', Collections.EMPTY_SET);
	}

	public static class MapTest extends DistortionMapObject {

		public MapTest() {
			super(new HashMap());

			Map map = map();

			map.put(5, "nk");
			map.put("12", 5);
			map.put('c', Collections.EMPTY_SET);
		}

		@Override public Map map() {
			return super.map();
		}
	}

	public static Set stupid_set = new HashSet();

	static {
		stupid_set.add("pop".getBytes());
		stupid_set.add("54");
		stupid_set.add(34);
	}

	public static class SetTest extends DistortionSetObject {

		public SetTest() {
			super(new HashSet());

			Set set = set();

			set.add("pop".getBytes());
			set.add("54");
			set.add(34);
		}

		@Override public Set set() {
			return super.set();
		}
	}

	public static NavigableMap stupid_nav_map = new TreeMap();

	static {
		stupid_nav_map.put("3", Collections.singletonMap(2, true));
		stupid_nav_map.put("false", 3);
		stupid_nav_map.put("yes", "no");
	}

	public static class NavMapTest extends DistortionNavigableMapObject {

		public NavMapTest() {
			super(new TreeMap<>());

			NavigableMap map = map();

			map.put("3", Collections.singletonMap(2, true));
			map.put("false", 3);
			map.put("yes", "no");
		}

		@Override public NavigableMap map() {
			return super.map();
		}
	}

	public static NavigableSet stupid_nav_set = new TreeSet();

	static {
		stupid_nav_set.add(1);
		stupid_nav_set.add(11);
		stupid_nav_set.add(111);
	}

	public static class NavSetTest extends DistortionNavigableSetObject {

		public NavSetTest() {
			super(new TreeSet());

			NavigableSet set = set();

			set.add(1);
			set.add(11);
			set.add(111);
		}

		@Override public NavigableSet set() {
			return super.set();
		}
	}
}
