package engineering.taikun.distortion;

import engineering.taikun.distortion.EmbeddedStructureTest.DiffValue.DiffType;
import engineering.taikun.distortion.SerializationUtil.SerializationContext;
import engineering.taikun.distortion.api.structures.DistortionListObject;
import engineering.taikun.distortion.serialization.api.ByteArray;
import engineering.taikun.distortion.serialization.api.ByteArrayFactory;
import engineering.taikun.distortion.serialization.imp.ArrayWrapper;
import engineering.taikun.distortion.serialization.imp.ArrayWrapperFactory;
import engineering.taikun.distortion.store.imp.DebugKV;
import engineering.taikun.distortion.structures.imp.DList;
import engineering.taikun.distortion.structures.imp.DMap;
import org.testng.annotations.Test;

import java.util.*;
import java.util.Map.Entry;

public class EmbeddedStructureTest {

	static boolean locked;

	static ByteArrayFactory<ArrayWrapper> factory = new ArrayWrapperFactory();
	static SerializationUtil<ArrayWrapper> util = new SerializationUtil<>(
			factory, (short) 1,
			b -> locked = b,
			() -> locked
	);

	static DebugKV kv = new DebugKV();

	@Test
	public static void main() {

		System.out.println("Embedded structure test");

		DMap<String, Object, ArrayWrapper> dmap;

		SerializationContext ctx = util.new SerializationContext(kv);

		title("basic pointer test");
		dmap = new DMap<>(kv, SerializationUtil.EMPTY_ARRAY, util, ctx, (short) 1);
		diff();

		title("one -> test_value");
		dmap.put("one", "test_value");
		diff();

		title("two -> [A, B, C]");
		dmap.put("two", Arrays.asList("A", "B", "C"));
		diff();

		title("three -> 4");
		dmap.put("three", 4);
		diff();

		title("four -> TDP[53, 463, 621]");
		dmap.put("four", new ThreeDimPoint(53, 463, 621));
		diff();

		title("five -> 4352.3245");
		dmap.put("five", 4352.3245);
		diff();

		title("six -> null");
		dmap.put("six", null);
		diff();

		title("seven -> *[A, B, C]");
		dmap.put("seven", dmap.get("two"));
		diff();

		assert_(dmap.toString().equals("{one=test_value, two=[A, B, C], three=4, four=[TDP: 53, 463, 621], five=4352.3245, six=null, seven=[A, B, C]}"));

		title("XXX[two]");
		dmap.remove("two");
		diff();

		assert_(dmap.toString().equals("{one=test_value, three=4, four=[TDP: 53, 463, 621], five=4352.3245, six=null, seven=[A, B, C]}"));

		title("Context close");
		ctx.close();
		diff();
		dmap = new DMap<>(kv, SerializationUtil.EMPTY_ARRAY, util, ctx);

		assert_(dmap.toString().equals("{one=test_value, three=4, four=[TDP: 53, 463, 621], five=4352.3245, six=null, seven=[A, B, C]}"));

		title("seven -> *TDP[53, 463, 621]");
		dmap.put("seven", dmap.get("four"));
		diff();

		assert_(dmap.toString().equals("{one=test_value, three=4, four=[TDP: 53, 463, 621], five=4352.3245, six=null, seven=[TDP: 53, 463, 621]}"));

		title("destroy");
		dmap.destroy();
		diff();

		title("Context close");
		ctx.close();
		diff();

		assert_(kv.map.isEmpty());

		title("basic moving test");
		dmap = new DMap<>(kv, SerializationUtil.EMPTY_ARRAY, util, ctx, (short) 1);
		diff();

		title("aaaaaaa -> test_value");
		dmap.put("aaaaaaa", "test_value");
		diff();

		title("bbbbbb -> [A, B, C]");
		dmap.put("bbbbbb", Arrays.asList("A", "B", "C"));
		diff();

		title("ccccc -> 4");
		dmap.put("ccccc", 4);
		diff();

		title("dddd -> TDP[53, 463, 621]");
		dmap.put("dddd", new ThreeDimPoint(53, 463, 621));
		diff();

		title("eee -> 4352.3245");
		dmap.put("eee", 4352.3245);
		diff();

		title("ff -> null");
		dmap.put("ff", null);
		diff();

		title("g -> *[A, B, C]");
		dmap.put("g", dmap.get("bbbbbb"));
		diff();

		assert_(dmap.toString().equals("{aaaaaaa=test_value, bbbbbb=[A, B, C], ccccc=4, dddd=[TDP: 53, 463, 621], eee=4352.3245, ff=null, g=[A, B, C]}"));

		title("dddd -> *[A, B, C]");
		Object old = dmap.put("dddd", dmap.get("bbbbbb"));
		diff();

		assert_(dmap.toString().equals("{aaaaaaa=test_value, bbbbbb=[A, B, C], ccccc=4, dddd=[A, B, C], eee=4352.3245, ff=null, g=[A, B, C]}"));

		title("aaaaaaa -> ~TDP[53, 463, 621]");
		dmap.put("aaaaaaa", old);
		diff();

		assert_(dmap.toString().equals("{aaaaaaa=[TDP: 53, 463, 621], bbbbbb=[A, B, C], ccccc=4, dddd=[A, B, C], eee=4352.3245, ff=null, g=[A, B, C]}"));

		title("Context close");
		ctx.close();
		diff();
		dmap = new DMap<>(kv, SerializationUtil.EMPTY_ARRAY, util, ctx);

		assert_(dmap.toString().equals("{aaaaaaa=[TDP: 53, 463, 621], bbbbbb=[A, B, C], ccccc=4, dddd=[A, B, C], eee=4352.3245, ff=null, g=[A, B, C]}"));

		title("destroy");
		dmap.destroy();
		diff();

		title("Context close");
		ctx.close();
		diff();

		assert_(kv.map.isEmpty());

		DList<Object, ArrayWrapper> dlist
				= new DList<>(kv, SerializationUtil.EMPTY_ARRAY, util, ctx, true);

		dlist.add("test_value");

		dlist.add(Arrays.asList("A", "B", "C"));

		dlist.add(4);

		dlist.add(new ThreeDimPoint(53, 463, 621));

		dlist.add(4352.3245);

		dlist.add(null);

		for (Object object : dlist) {
			System.out.println(object);
		}

		System.out.println("----");

		for (final Entry<ByteArray, ArrayWrapper> entry : kv.map.entrySet()) {
			System.out.println(entry);
		}

		System.out.println("-----");

		assert_(dlist.toString().equals("[test_value, [A, B, C], 4, [TDP: 53, 463, 621], 4352.3245, null]"));

		dlist.destroy();

		ctx.close();

		assert_(kv.map.isEmpty());

		System.out.println("passed");
	}

	public static void assert_(final boolean bool) {
		if (!bool) throw new RuntimeException();
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

	public static class ThreeDimPoint extends DistortionListObject<Integer> {

		public ThreeDimPoint(final int one, final int two, final int three) {
			super(new ArrayList<>(3));

			list().add(one);
			list().add(two);
			list().add(three);
		}

		public int getDimOne() {
			return list().get(0);
		}

		public int getDimTwo() {
			return list().get(1);
		}

		public int getDimThree() {
			return list().get(2);
		}

		@Override
		public String toString() {
			return "[TDP: " + getDimOne() + ", " + getDimTwo() + ", " + getDimThree() + ']';
		}
	}

}
