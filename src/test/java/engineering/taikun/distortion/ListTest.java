package engineering.taikun.distortion;

import engineering.taikun.distortion.serialization.api.ByteArrayFactory;
import engineering.taikun.distortion.serialization.imp.ArrayWrapper;
import engineering.taikun.distortion.serialization.imp.ArrayWrapperFactory;
import engineering.taikun.distortion.store.imp.DebugKV;
import engineering.taikun.distortion.structures.imp.DList;
import org.jetbrains.annotations.NotNull;
import org.testng.annotations.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This only tests for basic List correctness. Coverage on non-serialization stuff should be 100%.
 *
 * This exercises all paths of int-conditionals (x < y, x == y, x > y)
 *
 *
 */
public class ListTest {

	static boolean locked;

	static ByteArrayFactory<ArrayWrapper> factory = new ArrayWrapperFactory();
	static SerializationUtil<ArrayWrapper> util = new SerializationUtil<>(
			factory, (short) -1,
			b -> locked = b,
			() -> locked
	);

	static LinkedList<String> linkedlist = new LinkedList<>();
	static DebugKV kv = new DebugKV();
	static DList<String, ArrayWrapper> dlist
			= new DList<>(kv, SerializationUtil.EMPTY_ARRAY, util, util.new SerializationContext(kv), 200);

	static ListHarness harness = new ListHarness();

	@Test
	public static void main() {

		System.out.println("List test");

		// basic correctness

		harness.add("potato");

		harness.add("potato");

		harness.clear();

		harness.add("potato");

		harness.add("test");

		harness.add("tomato");

		harness.add("squash");

		harness.remove("tomato");

		harness.set(1, "ham");

		harness.add(0, "steak");

		harness.add(1, "kidney");

		harness.add("pie");

		harness.addAll(2, Arrays.asList("one", "two", "three", "four"));

		harness.remove(0);

		harness.remove(3);

		// basic wraparound

		for (int i = 0; i < 120; i++) {
			harness.add("value" + i);
			harness.remove(0);
		}

		for (int i = 120; i < 180; i++) {
			harness.add(0, "value" + i);
			harness.remove(harness.size() - 1);
		}

		// addall

		harness.addAll(5, Arrays.asList("uno1", "dos1", "tres1", "cuatro1", "cinco1", "seis1", "siete1", "ocho1", "nueve1", "diez1"));
		harness.addAll(5, Arrays.asList("uno2", "dos2", "tres2", "cuatro2", "cinco2", "seis2", "siete2", "ocho2", "nueve2", "diez2"));
		harness.addAll(5, Arrays.asList("uno3", "dos3", "tres3", "cuatro3", "cinco3", "seis3", "siete3", "ocho3", "nueve3", "diez3"));
		harness.addAll(5, Arrays.asList("uno4", "dos4", "tres4", "cuatro4", "cinco4", "seis4", "siete4", "ocho4", "nueve4", "diez4"));
		harness.addAll(5, Arrays.asList("uno5", "dos5", "tres5", "cuatro5", "cinco5", "seis5", "siete5", "ocho5", "nueve5", "diez5"));
		harness.addAll(5, Arrays.asList("uno6", "dos6", "tres6", "cuatro6", "cinco6", "seis6", "siete6", "ocho6", "nueve6", "diez6"));

		harness.set(66, "wraptest");

		for (int i = 0; i < 60; i++) {
			harness.remove(harness.size() - 1);
		}

		for (int i = 180; i < 335; i++) {
			harness.add(0, "value" + i);
			harness.remove(harness.size() - 1);
		}

		harness.addAll(0, Arrays.asList("uno7", "dos7", "tres7", "cuatro7", "cinco7", "seis7", "siete7", "ocho7", "nueve7", "diez7"));

		for (int i = 335; i < 335+183; i++) {
			harness.add("value" + i);
			harness.remove(0);
		}

		harness.addAll(18, Arrays.asList("uno8", "dos8", "tres8", "cuatro8", "cinco8", "seis8", "siete8", "ocho8", "nueve8", "diez8"));

		harness.addAll(28, Arrays.asList("uno9", "dos9", "tres9", "cuatro9", "cinco9", "seis9", "siete9", "ocho9", "nueve9", "diez9"));

		harness.remove(30);

		harness.remove(36);

		for (int i = 0; i < 122; i++) {
			harness.add(harness.remove(0));
		}

		harness.addAll(0, Arrays.asList("uno10", "dos10", "tres10", "cuatro10", "cinco10", "seis10", "siete10", "ocho10", "nueve10", "diez10"));

		harness.add(harness.size(), "cookie");

		for (int i = 0; i < 42; i++) {
			harness.add(0, harness.remove(harness.size() - 1));
		}

		harness.addAll(harness.size(), Arrays.asList("uno11", "dos11", "tres11", "cuatro11", "cinco11", "seis11", "siete11", "ocho11", "nueve11", "diez11"));

		harness.addAll(51, Arrays.asList("uno12", "dos12", "tres12", "cuatro12", "cinco12", "seis12", "siete12", "ocho12", "nueve12", "diez12"));

		for (int i = 0; i < 27; i++) {
			harness.remove(harness.size() - 1);
		}

		harness.addAll(harness.size(), Arrays.asList("uno13", "dos13", "tres13", "cuatro13", "cinco13", "seis13", "siete13", "ocho13", "nueve13", "diez13"));

		harness.addAll(harness.size(), Arrays.asList("uno14", "dos14", "tres14", "cuatro14", "cinco14", "seis14", "siete14", "ocho14", "nueve14", "diez14"));

		harness.addAll(45, Arrays.asList("uno15", "dos15", "tres15", "cuatro15", "cinco15", "seis15", "siete15", "ocho15", "nueve15", "diez15"));

		harness.addAll(20, Arrays.asList("uno16", "dos16", "tres16", "cuatro16", "cinco16", "seis16", "siete16", "ocho16", "nueve16", "diez16"));

		for (int i = 0; i < 35; i++) {
			harness.remove(harness.size() - 1);
		}

		harness.addAll(44, Arrays.asList("uno17", "dos17", "tres17", "cuatro17", "cinco17", "seis17", "siete17", "ocho17", "nueve17", "diez17"));

		for (int i = 0; i < 14; i++) {
			harness.remove(harness.size() - 1);
		}

		harness.addAll(20, Arrays.asList("uno18", "dos18", "tres18", "cuatro18", "cinco18", "seis18", "siete18", "ocho18", "nueve18", "diez18"));

		harness.addAll(10, Collections.EMPTY_LIST);

		// set

		harness.set(50, "cake");

		// remove

		harness.addAll(harness.size(), Arrays.asList("uno19", "dos19", "tres19", "cuatro19", "cinco19", "seis19", "siete19", "ocho19", "nueve19", "diez19"));
		harness.addAll(harness.size(), Arrays.asList("uno20", "dos20", "tres20", "cuatro20", "cinco20", "seis20", "siete20", "ocho20", "nueve20", "diez20"));

		harness.remove(60);

		for (int i = 0; i < 25; i++) {
			harness.remove(3);
		}

		// deque stuff

		// getlast

		harness.getLast();

		// removelast

		harness.addAll(harness.size(), Arrays.asList("uno21", "dos21", "tres21", "cuatro21", "cinco21", "seis21", "siete21", "ocho21", "nueve21", "diez21"));

		harness.removeLast();
		harness.removeLast();
		harness.removeLast();
		harness.removeLast();
		harness.removeLast();
		harness.removeLast();
		harness.removeLast();

		// addall

		harness.addAll(Collections.EMPTY_LIST);

		harness.addAll(Arrays.asList("uno22", "dos22", "tres22", "cuatro22", "cinco22", "seis22", "siete22", "ocho22", "nueve22", "diez22"));

		for (int i = 0; i < 18; i++) {
			harness.removeLast();
		}

		harness.addAll(Arrays.asList("uno23", "dos23", "tres23", "cuatro23", "cinco23", "seis23", "siete23", "ocho23", "nueve23", "diez23"));

		harness.addAll(Arrays.asList("uno24", "dos24", "tres24", "cuatro24", "cinco24", "seis24", "siete24", "ocho24", "nueve24", "diez24"));
		harness.addAll(Arrays.asList("uno25", "dos25", "tres25", "cuatro25", "cinco25", "seis25", "siete25", "ocho25", "nueve25", "diez25"));

		System.out.println("Start: " + util.new IntUnion(kv.read(util.new IntUnion(-2).toBA())).getInt());
		System.out.println("End: " + util.new IntUnion(kv.read(util.new IntUnion(-3).toBA())).getInt());

		// reset (because i forgot to override addall originally)

		for (int i = 0; i < 22; i++) {
			harness.removeLast();
		}

		// out of bounds

		try {
			dlist.addAll(-1, Collections.EMPTY_LIST);
			throw new RuntimeException("Failed to throw exception");
		} catch (IndexOutOfBoundsException e) {}

		try {
			dlist.addAll(100, Collections.EMPTY_LIST);
			throw new RuntimeException("Failed to throw exception");
		} catch (IndexOutOfBoundsException e) {}

		try {
			dlist.add(100, "donut");
			throw new RuntimeException("Failed to throw exception");
		} catch (IndexOutOfBoundsException e) {}

		try {
			dlist.get(-1);
			throw new RuntimeException("Failed to throw exception");
		} catch (IndexOutOfBoundsException e) {}

		try {
			dlist.get(100);
			throw new RuntimeException("Failed to throw exception");
		} catch (IndexOutOfBoundsException e) {}

		try {
			dlist.set(-1, "chocolate");
			throw new RuntimeException("Failed to throw exception");
		} catch (IndexOutOfBoundsException e) {}

		try {
			dlist.set(100, "toffee");
			throw new RuntimeException("Failed to throw exception");
		} catch (IndexOutOfBoundsException e) {}

		try {
			dlist.remove(-1);
			throw new RuntimeException("Failed to throw exception");
		} catch (IndexOutOfBoundsException e) {}

		try {
			dlist.remove(49);
			throw new RuntimeException("Failed to throw exception");
		} catch (IndexOutOfBoundsException e) {}

		harness.addAll(harness.size(), Arrays.asList("uno22", "dos22", "tres22", "cuatro22", "cinco22", "seis22", "siete22", "ocho22", "nueve22", "diez22"));

		try {
			dlist.remove(100);
			throw new RuntimeException("Failed to throw exception");
		} catch (IndexOutOfBoundsException e) {}

		dlist.clear();

		try {
			dlist.remove(0);
			throw new RuntimeException("Failed to throw exception");
		} catch (IndexOutOfBoundsException e) {}

		try {
			dlist.removeLast();
			throw new RuntimeException("Failed to throw exception");
		} catch (NoSuchElementException e) {}

		try {
			dlist.getLast();
			throw new RuntimeException("Failed to throw exception");
		} catch (NoSuchElementException e) {}

		// illegal state (full)

		dlist.addAll(0, Arrays.asList("uno22", "dos22", "tres22", "cuatro22", "cinco22", "seis22", "siete22", "ocho22", "nueve22", "diez22"));

		dlist.removeFirst();
		dlist.removeFirst();

		for (int i = 0; i < 191; i++) {
			dlist.add("junk");
		}

		System.out.println("Size: " + dlist.size());

		try {
			dlist.add("cream");
			throw new RuntimeException("Failed to throw exception");
		} catch (IllegalStateException e) {}

		// test near-full compliance
		dlist.removeLast();
		dlist.add("cream");

		dlist.removeFirst();
		dlist.add("sugar");

		System.out.println("Size: " + dlist.size());

		try {
			dlist.add("sprinkles");
			throw new RuntimeException("Failed to throw exception");
		} catch (IllegalStateException e) {}

		// test near-full compliance
		dlist.removeLast();
		dlist.add("sprinkles");

		dlist.removeFirst();
		dlist.add("sugar");

		System.out.println("Size: " + dlist.size());

		try {
			dlist.add("chips");
			throw new RuntimeException("Failed to throw exception");
		} catch (IllegalStateException e) {}

		// test near-full compliance
		dlist.removeLast();
		dlist.add("chips");

		System.out.println("Size: " + dlist.size());

		try {
			dlist.addAll(0, IntStream.range(0, 200).mapToObj(Integer::toString).collect(Collectors.toList()));
			throw new RuntimeException("Failed to throw exception");
		} catch (IllegalStateException e) {}

		dlist.removeLast();
		dlist.addFirst("sugar");
		dlist.removeLast();
		dlist.addFirst("sugar");

		System.out.println("Size: " + dlist.size());

		try {
			dlist.add(0, "icing");
			throw new RuntimeException("Failed to throw exception");
		} catch (IllegalStateException e) {}

		// test near-full compliance
		dlist.removeFirst();
		dlist.add(0, "icing");

		dlist.removeFirst();
		dlist.add("sugar");

		System.out.println("Size: " + dlist.size());

		try {
			dlist.add(0, "cocoa");
			throw new RuntimeException("Failed to throw exception");
		} catch (IllegalStateException e) {}

		// test near-full compliance
		dlist.removeFirst();
		dlist.add("cocoa");

		dlist.removeFirst();
		dlist.add("sugar");

		System.out.println("Size: " + dlist.size());

		try {
			dlist.add(0, "marshmallows");
			throw new RuntimeException("Failed to throw exception");
		} catch (IllegalStateException e) {}

		// test near-full compliance
		dlist.removeFirst();
		dlist.add(0, "marshmallows");

		try {
			dlist.add(1, "chili powder");
			throw new RuntimeException("Failed to throw exception");
		} catch (IllegalStateException e) {}

		// test near-full compliance
		dlist.remove(1);
		dlist.add("chili powder");

		try {
			dlist.addAll(IntStream.range(0, 200).mapToObj(Integer::toString).collect(Collectors.toList()));
			throw new RuntimeException("Failed to throw exception");
		} catch (IllegalStateException e) {}

		dlist.removeLast();
		dlist.addFirst("sugar");
		dlist.removeLast();
		dlist.addFirst("sugar");

		System.out.println("Size: " + dlist.size());

		System.out.println("Start: " + util.new IntUnion(kv.read(util.new IntUnion(-2).toBA())).getInt());
		System.out.println("End: " + util.new IntUnion(kv.read(util.new IntUnion(-3).toBA())).getInt());

		try {
			dlist.addAll(Collections.singleton("cinnamon"));
			throw new RuntimeException("Failed to throw exception");
		} catch (IllegalStateException e) {}

		// test near-full compliance
		dlist.removeLast();
		dlist.addAll(Collections.singleton("cinnamon"));

		System.out.println("Size: " + dlist.size());

		dlist.removeLast();
		dlist.addFirst("sugar");

		System.out.println("Size: " + dlist.size());

		System.out.println("Start: " + util.new IntUnion(kv.read(util.new IntUnion(-2).toBA())).getInt());
		System.out.println("End: " + util.new IntUnion(kv.read(util.new IntUnion(-3).toBA())).getInt());

		try {
			dlist.addAll(Collections.singleton("mint"));
			throw new RuntimeException("Failed to throw exception");
		} catch (IllegalStateException e) {}

		// test near-full compliance
		dlist.removeLast();
		dlist.addAll(Collections.singleton("mint"));

		dlist.removeLast();
		dlist.addFirst("sugar");

		try {
			dlist.addAll(Collections.singleton("nibs"));
			throw new RuntimeException("Failed to throw exception");
		} catch (IllegalStateException e) {}

		// test near-full compliance
		dlist.removeLast();
		dlist.addAll(Collections.singleton("nibs"));

		System.out.println("Start: " + util.new IntUnion(kv.read(util.new IntUnion(-2).toBA())).getInt());
		System.out.println("End: " + util.new IntUnion(kv.read(util.new IntUnion(-3).toBA())).getInt());

		dlist.removeFirst();
		dlist.add("sugar");
		dlist.removeFirst();
		dlist.add("sugar");
		dlist.removeFirst();
		dlist.add("sugar");

		System.out.println("Start: " + util.new IntUnion(kv.read(util.new IntUnion(-2).toBA())).getInt());
		System.out.println("End: " + util.new IntUnion(kv.read(util.new IntUnion(-3).toBA())).getInt());

		try {
			dlist.addAll(Collections.singleton("syrup"));
			throw new RuntimeException("Failed to throw exception");
		} catch (IllegalStateException e) {}

		// test near-full compliance
		dlist.removeLast();

		System.out.println("Start: " + util.new IntUnion(kv.read(util.new IntUnion(-2).toBA())).getInt());
		System.out.println("End: " + util.new IntUnion(kv.read(util.new IntUnion(-3).toBA())).getInt());

		dlist.addAll(Collections.singleton("syrup"));

		System.out.println("Start: " + util.new IntUnion(kv.read(util.new IntUnion(-2).toBA())).getInt());
		System.out.println("End: " + util.new IntUnion(kv.read(util.new IntUnion(-3).toBA())).getInt());

		dlist.destroy();

		assert_(kv.map.isEmpty());

		System.out.println("passed");
	}

	public static void assert_(final boolean bool) {
		if (!bool) throw new RuntimeException();
	}

	public static class ListHarness implements List<String>, Deque<String> {

		@Override
		public int size() {
			final int linkedresult = linkedlist.size();
			final int dresult = dlist.size();

			assert_(linkedresult == dresult);
			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			assert_(kv.map.size() == dresult + 2);

			return linkedresult;
		}

		@Override
		public boolean isEmpty() {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean contains(final Object o) {
			throw new UnsupportedOperationException();
		}

		@NotNull
		@Override
		public Iterator<String> iterator() {
			throw new UnsupportedOperationException();
		}

		@NotNull @Override public Iterator<String> descendingIterator() {
			throw new UnsupportedOperationException();
		}

		@NotNull
		@Override
		public Object[] toArray() {
			throw new UnsupportedOperationException();
		}

		@NotNull
		@Override
		public <T> T[] toArray(final T[] a) {
			throw new UnsupportedOperationException();
		}

		@Override public void addFirst(final String s) {
			throw new UnsupportedOperationException();
		}

		@Override public void addLast(final String s) {
			throw new UnsupportedOperationException();
		}

		@Override public boolean offerFirst(final String s) {
			throw new UnsupportedOperationException();
		}

		@Override public boolean offerLast(final String s) {
			throw new UnsupportedOperationException();
		}

		@Override public String removeFirst() {
			throw new UnsupportedOperationException();
		}

		@Override public String removeLast() {
			final String linkedresult = linkedlist.removeLast();
			final String dresult = dlist.removeLast();

			assert_(Objects.equals(linkedresult, dresult));
			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			assert_(kv.map.size() == size() + 2);

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}

			return linkedresult;
		}

		@Override public String pollFirst() {
			throw new UnsupportedOperationException();
		}

		@Override public String pollLast() {
			throw new UnsupportedOperationException();
		}

		@Override public String getFirst() {
			throw new UnsupportedOperationException();
		}

		@Override public String getLast() {
			final String linkedresult = linkedlist.getLast();
			final String dresult = dlist.getLast();

			assert_(Objects.equals(linkedresult, dresult));
			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			assert_(kv.map.size() == size() + 2);

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}

			return linkedresult;
		}

		@Override public String peekFirst() {
			throw new UnsupportedOperationException();
		}

		@Override public String peekLast() {
			throw new UnsupportedOperationException();
		}

		@Override public boolean removeFirstOccurrence(final Object o) {
			throw new UnsupportedOperationException();
		}

		@Override public boolean removeLastOccurrence(final Object o) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean add(final String s) {
			final boolean linkedresult = linkedlist.add(s);
			final boolean dresult = dlist.add(s);

			assert_(linkedresult == dresult);
			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			assert_(kv.map.size() == size() + 2);

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}

			return linkedresult;
		}

		@Override public boolean offer(final String s) {
			throw new UnsupportedOperationException();
		}

		@Override public String remove() {
			throw new UnsupportedOperationException();
		}

		@Override public String poll() {
			throw new UnsupportedOperationException();
		}

		@Override public String element() {
			throw new UnsupportedOperationException();
		}

		@Override public String peek() {
			throw new UnsupportedOperationException();
		}

		@Override public void push(final String s) {
			throw new UnsupportedOperationException();
		}

		@Override public String pop() {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean remove(final Object o) {
			final boolean linkedresult = linkedlist.remove(o);
			final boolean dresult = dlist.remove(o);

			assert_(linkedresult == dresult);
			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			assert_(kv.map.size() == size() + 2);

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}

			return linkedresult;
		}

		@Override
		public boolean containsAll(final Collection<?> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean addAll(final Collection<? extends String> c) {
			final boolean linkedresult = linkedlist.addAll(c);
			final boolean dresult = dlist.addAll(c);

			assert_(linkedresult == dresult);
			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			assert_(kv.map.size() == size() + 2);

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}

			return linkedresult;
		}

		@Override
		public boolean addAll(final int index, final Collection<? extends String> c) {
			final boolean linkedresult = linkedlist.addAll(index, c);
			final boolean dresult = dlist.addAll(index, c);

			assert_(linkedresult == dresult);
			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			assert_(kv.map.size() == size() + 2);

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}

			return linkedresult;
		}

		@Override
		public boolean removeAll(final Collection<?> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean retainAll(final Collection<?> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void clear() {
			linkedlist.clear();
			dlist.clear();

			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			assert_(kv.map.size() == size() + 2);

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}
		}

		@Override
		public String get(final int index) {
			final String linkedresult = linkedlist.get(index);
			final String dresult = dlist.get(index);

			assert_(Objects.equals(linkedresult, dresult));
			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			assert_(kv.map.size() == size() + 2);

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}

			return linkedresult;
		}

		@Override
		public String set(final int index, final String element) {
			final String linkedresult = linkedlist.set(index, element);
			final String dresult = dlist.set(index, element);

			assert_(Objects.equals(linkedresult, dresult));
			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			assert_(kv.map.size() == size() + 2);

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}

			return linkedresult;
		}

		@Override
		public void add(final int index, final String element) {
			linkedlist.add(index, element);
			dlist.add(index, element);

			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			assert_(kv.map.size() == size() + 2);

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}
		}

		@Override
		public String remove(final int index) {
			final String linkedresult = linkedlist.remove(index);
			final String dresult = dlist.remove(index);

			assert_(Objects.equals(linkedresult, dresult));
			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			assert_(kv.map.size() == size() + 2);

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}

			return linkedresult;
		}

		@Override
		public int indexOf(final Object o) {
			throw new UnsupportedOperationException();
		}

		@Override
		public int lastIndexOf(final Object o) {
			throw new UnsupportedOperationException();
		}

		@NotNull
		@Override
		public ListIterator<String> listIterator() {
			throw new UnsupportedOperationException();
		}

		@NotNull
		@Override
		public ListIterator<String> listIterator(final int index) {
			throw new UnsupportedOperationException();
		}

		@NotNull
		@Override
		public List<String> subList(final int fromIndex, final int toIndex) {
			throw new UnsupportedOperationException();
		}
	}


}
