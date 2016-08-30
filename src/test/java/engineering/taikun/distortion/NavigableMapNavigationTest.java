package engineering.taikun.distortion;

import engineering.taikun.combinations.Combinator;
import engineering.taikun.distortion.NavigableMapTest.NavigableMapHarness;
import engineering.taikun.distortion.serialization.api.ByteArrayFactory;
import engineering.taikun.distortion.serialization.imp.ArrayWrapper;
import engineering.taikun.distortion.serialization.imp.ArrayWrapperFactory;
import engineering.taikun.distortion.store.imp.DebugKV;
import engineering.taikun.distortion.structures.imp.DNavigableMap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import static engineering.taikun.combinations.Tuple.t;

public class NavigableMapNavigationTest {

	static boolean locked;

	static ByteArrayFactory<ArrayWrapper> factory = new ArrayWrapperFactory();
	static SerializationUtil<ArrayWrapper> util = new SerializationUtil<>(
			factory, (short) 1,
			b -> locked = b,
			() -> locked
	);

	@DataProvider(name = "navdat")
//	@DataProvider(name = "navdat", parallel = true)
	public static Object[][] data() {
		return new Combinator()
				.chooseOne(
						t(true, false, false),
						new Combinator()
								.chooseOne(false)
								.chooseOne(true, false)
								.chooseOne(true, false)
				)
				.chooseOne(
						t(true, false, false),
						new Combinator()
								.chooseOne(false)
								.chooseOne(true, false)
								.chooseOne(true, false)
				)
				.chooseOne(true, false)
				.array();
	}

	@DataProvider(name = "testdat")
	public static Object[][] testdat() {
		return new Object[][] {
				{ true, false, false, true, false, false, false }
		};
	}

	@Test(dataProvider = "navdat")
//	@Test(dataProvider = "testdat")
	public static void main(
			final boolean headmap, final boolean head_overlap, final boolean head_inclusive,
			final boolean tailmap, final boolean tail_overlap, final boolean tail_inclusive,
			final boolean backwards
	) {

		System.out.println("Sorted map navigation test");

		TreeMap<Integer, Integer> treemap = new TreeMap<>();
		DebugKV kv = new DebugKV();
		DNavigableMap<Integer, Integer, ArrayWrapper> dmap = new DNavigableMap<>(
				kv, SerializationUtil.EMPTY_ARRAY, util, util.new SerializationContext(kv), (short) 8, null
		);

		NavigableMapHarness<Integer, Integer> harness = new NavigableMapHarness<>(treemap, dmap);
		Random random = new Random(543346);

		for (int i = 0; i < 10000; i += 10) {
			harness.put(i, random.nextInt());
		}

		final int fromkey = head_overlap ? 100 : 75;
		final int tokey = tail_overlap ? 9000 : 9001;

		if (headmap && tailmap) {
			if (backwards) {
				test(harness.descendingMap(), true);
			} else {
				test(harness, false);
			}
		} else if (headmap) {
			if (backwards) {
				test(harness.headMap(tokey, tail_inclusive).descendingMap(), true);
			} else {
				test(harness.headMap(tokey, tail_inclusive), false);
			}
		} else if (tailmap) {
			if (backwards) {
				test(harness.tailMap(fromkey, head_inclusive).descendingMap(), true);
			} else {
				test(harness.tailMap(fromkey, head_inclusive), false);
			}
		} else {
			if (backwards) {
				test(harness.subMap(fromkey, head_inclusive, tokey, tail_inclusive).descendingMap(), true);
			} else {
				test(harness.subMap(fromkey, head_inclusive, tokey, tail_inclusive), false);
			}
		}

		System.out.println("passed");
	}

	public static void test(final NavigableMap<Integer, Integer> map, final boolean backwards) {
		map.containsKey(-1);
		map.containsKey(0);
		map.containsKey(75);
		map.containsKey(100);
		map.containsKey(5000);
		map.containsKey(9000);
		map.containsKey(9001);
		map.containsKey(9990);
		map.containsKey(10000);

		map.get(-1);
		map.get(0);
		map.get(75);
		map.get(100);
		map.get(5000);
		map.get(9000);
		map.get(9001);
		map.get(9990);
		map.get(10000);

		map.remove(-1);
		map.remove(0);
		map.remove(75);
		map.remove(100);
		map.remove(5000);
		map.remove(9000);
		map.remove(9001);
		map.remove(9990);
		map.remove(10000);

		map.put(-1, 2000);
		map.put(0, 2001);
		map.put(75, 2002);
		map.put(100, 2003);
		map.put(5000, 2004);
		map.put(9000, 2005);
		map.put(9001, 2006);
		map.put(9990, 2007);
		map.put(10000, 2008);

		map.remove(-1);
		map.remove(75);
		map.remove(9001);
		map.remove(10000);

		map.lowerEntry(-1);
		map.lowerEntry(0);
		map.lowerEntry(75);
		map.lowerEntry(100);
		map.lowerEntry(5000);
		map.lowerEntry(9000);
		map.lowerEntry(9001);
		map.lowerEntry(9990);
		map.lowerEntry(10000);

		map.lowerKey(-1);
		map.lowerKey(0);
		map.lowerKey(75);
		map.lowerKey(100);
		map.lowerKey(5000);
		map.lowerKey(9000);
		map.lowerKey(9001);
		map.lowerKey(9990);
		map.lowerKey(10000);

		map.floorEntry(-1);
		map.floorEntry(0);
		map.floorEntry(75);
		map.floorEntry(100);
		map.floorEntry(5000);
		map.floorEntry(9000);
		map.floorEntry(9001);
		map.floorEntry(9990);
		map.floorEntry(10000);

		map.floorKey(-1);
		map.floorKey(0);
		map.floorKey(75);
		map.floorKey(100);
		map.floorKey(5000);
		map.floorKey(9000);
		map.floorKey(9001);
		map.floorKey(9990);
		map.floorKey(10000);

		map.ceilingEntry(-1);
		map.ceilingEntry(0);
		map.ceilingEntry(75);
		map.ceilingEntry(100);
		map.ceilingEntry(5000);
		map.ceilingEntry(9000);
		map.ceilingEntry(9001);
		map.ceilingEntry(9990);
		map.ceilingEntry(10000);

		map.ceilingKey(-1);
		map.ceilingKey(0);
		map.ceilingKey(75);
		map.ceilingKey(100);
		map.ceilingKey(5000);
		map.ceilingKey(9000);
		map.ceilingKey(9001);
		map.ceilingKey(9990);
		map.ceilingKey(10000);

		map.higherEntry(-1);
		map.higherEntry(0);
		map.higherEntry(75);
		map.higherEntry(100);
		map.higherEntry(5000);
		map.higherEntry(9000);
		map.higherEntry(9001);
		map.higherEntry(9990);
		map.higherEntry(10000);

		map.higherKey(-1);
		map.higherKey(0);
		map.higherKey(75);
		map.higherKey(100);
		map.higherKey(5000);
		map.higherKey(9000);
		map.higherKey(9001);
		map.higherKey(9990);
		map.higherKey(10000);

		Entry<Integer, Integer> entry1 = map.pollFirstEntry();
		map.put(entry1.getKey(), entry1.getValue());

		map.firstEntry();

		map.firstKey();

		Entry<Integer, Integer> entry2 = map.pollLastEntry();
		map.put(entry2.getKey(), entry2.getValue());

		map.lastEntry();

		map.lastKey();

		NavigableMap temp1 = map.descendingMap();

		test2(temp1, !backwards);

		// 200/175
		// 8900/8905

		NavigableMap temp2;
		NavigableMap temp3;
		NavigableMap temp4;
		NavigableMap temp5;

		if (backwards) {

			temp2 = map.subMap(8900, true, 200, true);
			temp3 = map.subMap(8900, false, 200, false);
			temp4 = map.subMap(8905, true, 175, true);
			temp5 = map.subMap(8905, false, 175, false);

		} else {

			temp2 = map.subMap(200, true, 8900, true);
			temp3 = map.subMap(200, false, 8900, false);
			temp4 = map.subMap(175, true, 8905, true);
			temp5 = map.subMap(175, false, 8905, false);

		}

		test2(temp2, backwards);
		test2(temp3, backwards);
		test2(temp4, backwards);
		test2(temp5, backwards);

		// overlap

		// 100/75
		// 9000/9001

		NavigableMap temp6;
		NavigableMap temp7;
		NavigableMap temp8;
		NavigableMap temp9;

		if (backwards) {

			temp6 = map.subMap(9000, true, 100, true);
			temp7 = map.subMap(9000, false, 100, false);
			temp8 = map.subMap(9001, true, 75, true);
			temp9 = map.subMap(9001, false, 75, false);

		} else {

			temp6 = map.subMap(100, true, 9000, true);
			temp7 = map.subMap(100, false, 9000, false);
			temp8 = map.subMap(75, true, 9001, true);
			temp9 = map.subMap(75, false, 9001, false);

		}

		if (temp6 != null) test2(temp6, backwards);
		if (temp7 != null) test2(temp7, backwards);
		if (temp8 != null) test2(temp8, backwards);
		if (temp9 != null) test2(temp9, backwards);

		// overhang

		NavigableMap temp10;

		if (backwards) {

			temp10 = map.subMap(10000, true, 0, true);

		} else {

			temp10 = map.subMap(0, true, 10000, true);

		}

		if (temp10 != null) {
			test2(temp10, backwards);
		}

		NavigableMap temp11;
		NavigableMap temp12;
		NavigableMap temp13;
		NavigableMap temp14;

		if (backwards) {

			temp11 = map.tailMap(8900, true);
			temp12 = map.tailMap(8900, false);
			temp13 = map.tailMap(8905, true);
			temp14 = map.tailMap(8905, false);

		} else {

			temp11 = map.headMap(8900, true);
			temp12 = map.headMap(8900, false);
			temp13 = map.headMap(8905, true);
			temp14 = map.headMap(8905, false);

		}

		test2(temp11, backwards);
		test2(temp12, backwards);
		test2(temp13, backwards);
		test2(temp14, backwards);

		NavigableMap temp15;
		NavigableMap temp16;
		NavigableMap temp17;
		NavigableMap temp18;

		if (backwards) {

			temp15 = map.headMap(200, true);
			temp16 = map.headMap(200, false);
			temp17 = map.headMap(175, true);
			temp18 = map.headMap(175, false);

		} else {

			temp15 = map.tailMap(200, true);
			temp16 = map.tailMap(200, false);
			temp17 = map.tailMap(175, true);
			temp18 = map.tailMap(175, false);

		}

		test2(temp15, backwards);
		test2(temp16, backwards);
		test2(temp17, backwards);
		test2(temp18, backwards);
	}

	public static void test2(final NavigableMap<Integer, Integer> map, final boolean backwards) {
		map.containsKey(-1);
		map.containsKey(0);
		map.containsKey(75);
		map.containsKey(100);
		map.containsKey(175);
		map.containsKey(200);
		map.containsKey(5000);
		map.containsKey(8900);
		map.containsKey(8905);
		map.containsKey(9000);
		map.containsKey(9001);
		map.containsKey(9990);
		map.containsKey(10000);

		map.get(-1);
		map.get(0);
		map.get(75);
		map.get(100);
		map.get(175);
		map.get(200);
		map.get(5000);
		map.get(8900);
		map.get(8905);
		map.get(9000);
		map.get(9001);
		map.get(9990);
		map.get(10000);

		map.remove(-1);
		map.remove(0);
		map.remove(75);
		map.remove(100);
		map.remove(175);
		map.remove(200);
		map.remove(5000);
		map.remove(8900);
		map.remove(8905);
		map.remove(9000);
		map.remove(9001);
		map.remove(9990);
		map.remove(10000);

		map.put(-1, 2000);
		map.put(0, 2001);
		map.put(75, 2002);
		map.put(100, 2003);
		map.put(175, 3000);
		map.put(200, 3001);
		map.put(5000, 2004);
		map.put(5000, 2004);
		map.put(8900, 3002);
		map.put(8905, 3003);
		map.put(9001, 2006);
		map.put(9990, 2007);
		map.put(10000, 2008);

		map.remove(-1);
		map.remove(75);
		map.remove(175);
		map.remove(8905);
		map.remove(9001);
		map.remove(10000);

		map.lowerEntry(-1);
		map.lowerEntry(0);
		map.lowerEntry(75);
		map.lowerEntry(100);
		map.lowerEntry(175);
		map.lowerEntry(200);
		map.lowerEntry(5000);
		map.lowerEntry(8900);
		map.lowerEntry(8905);
		map.lowerEntry(9000);
		map.lowerEntry(9001);
		map.lowerEntry(9990);
		map.lowerEntry(10000);

		map.lowerKey(-1);
		map.lowerKey(0);
		map.lowerKey(75);
		map.lowerKey(100);
		map.lowerKey(175);
		map.lowerKey(200);
		map.lowerKey(5000);
		map.lowerKey(8900);
		map.lowerKey(8905);
		map.lowerKey(9000);
		map.lowerKey(9001);
		map.lowerKey(9990);
		map.lowerKey(10000);

		map.floorEntry(-1);
		map.floorEntry(0);
		map.floorEntry(75);
		map.floorEntry(100);
		map.floorEntry(175);
		map.floorEntry(200);
		map.floorEntry(5000);
		map.floorEntry(8900);
		map.floorEntry(8905);
		map.floorEntry(9000);
		map.floorEntry(9001);
		map.floorEntry(9990);
		map.floorEntry(10000);

		map.floorKey(-1);
		map.floorKey(0);
		map.floorKey(75);
		map.floorKey(100);
		map.floorKey(175);
		map.floorKey(200);
		map.floorKey(5000);
		map.floorKey(8900);
		map.floorKey(8905);
		map.floorKey(9000);
		map.floorKey(9001);
		map.floorKey(9990);
		map.floorKey(10000);

		map.ceilingEntry(-1);
		map.ceilingEntry(0);
		map.ceilingEntry(75);
		map.ceilingEntry(100);
		map.ceilingEntry(175);
		map.ceilingEntry(200);
		map.ceilingEntry(5000);
		map.ceilingEntry(8900);
		map.ceilingEntry(8905);
		map.ceilingEntry(9000);
		map.ceilingEntry(9001);
		map.ceilingEntry(9990);
		map.ceilingEntry(10000);

		map.ceilingKey(-1);
		map.ceilingKey(0);
		map.ceilingKey(75);
		map.ceilingKey(100);
		map.ceilingKey(175);
		map.ceilingKey(200);
		map.ceilingKey(5000);
		map.ceilingKey(8900);
		map.ceilingKey(8905);
		map.ceilingKey(9000);
		map.ceilingKey(9001);
		map.ceilingKey(9990);
		map.ceilingKey(10000);

		map.higherEntry(-1);
		map.higherEntry(0);
		map.higherEntry(75);
		map.higherEntry(100);
		map.higherEntry(175);
		map.higherEntry(200);
		map.higherEntry(5000);
		map.higherEntry(8900);
		map.higherEntry(8905);
		map.higherEntry(9000);
		map.higherEntry(9001);
		map.higherEntry(9990);
		map.higherEntry(10000);

		map.higherKey(-1);
		map.higherKey(0);
		map.higherKey(75);
		map.higherKey(100);
		map.higherKey(175);
		map.higherKey(200);
		map.higherKey(5000);
		map.higherKey(8900);
		map.higherKey(8905);
		map.higherKey(9000);
		map.higherKey(9001);
		map.higherKey(9990);
		map.higherKey(10000);

		Entry<Integer, Integer> entry1 = map.pollFirstEntry();
		map.put(entry1.getKey(), entry1.getValue());

		map.firstEntry();

		map.firstKey();

		Entry<Integer, Integer> entry2 = map.pollLastEntry();
		map.put(entry2.getKey(), entry2.getValue());

		map.lastEntry();

		map.lastKey();

		map.descendingMap();

		if (backwards) {

			map.subMap(8900, true, 200, true);
			map.subMap(8900, false, 200, false);
			map.subMap(8905, true, 175, true);
			map.subMap(8905, false, 175, false);

			map.subMap(9000, true, 100, true);
			map.subMap(9000, false, 100, false);
			map.subMap(9001, true, 75, true);
			map.subMap(9001, false, 75, false);

			map.subMap(10000, true, 0, true);

			map.tailMap(8900, true);
			map.tailMap(8900, false);
			map.tailMap(8905, true);
			map.tailMap(8905, false);

			map.headMap(200, true);
			map.headMap(200, false);
			map.headMap(175, true);
			map.headMap(175, false);


		} else {

			map.subMap(200, true, 8900, true);
			map.subMap(200, false, 8900, false);
			map.subMap(175, true, 8905, true);
			map.subMap(175, false, 8905, false);

			map.subMap(100, true, 9000, true);
			map.subMap(100, false, 9000, false);
			map.subMap(75, true, 9001, true);
			map.subMap(75, false, 9001, false);

			map.subMap(0, true, 10000, true);

			map.headMap(8900, true);
			map.headMap(8900, false);
			map.headMap(8905, true);
			map.headMap(8905, false);

			map.tailMap(200, true);
			map.tailMap(200, false);
			map.tailMap(175, true);
			map.tailMap(175, false);

		}

	}

}
