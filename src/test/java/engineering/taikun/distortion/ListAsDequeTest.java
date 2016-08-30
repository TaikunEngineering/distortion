package engineering.taikun.distortion;

import engineering.taikun.distortion.serialization.api.ByteArrayFactory;
import engineering.taikun.distortion.serialization.imp.ArrayWrapper;
import engineering.taikun.distortion.serialization.imp.ArrayWrapperFactory;
import engineering.taikun.distortion.store.imp.DebugKV;
import engineering.taikun.distortion.structures.imp.DList;
import org.jetbrains.annotations.NotNull;
import org.testng.annotations.Test;

import java.util.*;

/**
 * Only tests the basic Deque methods, for in depth test of DList correctness, look to ListTest
 */
public class ListAsDequeTest {

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

	static DequeHarness harness = new DequeHarness();

	@Test
	public static void main() {
		System.out.println("List as deque test");

		harness.add("oak");

		harness.add("pine");

		harness.addFirst("cherry");

		harness.addLast("maple");

		harness.add("walnut");

		harness.removeFirst();

		harness.removeLast();

		harness.pollFirst();

		harness.pollLast();

		// CLEAR
		harness.clear();

		harness.add("oak");

		harness.add("pine");

		harness.add("cherry");

		harness.add("maple");

		harness.pollFirst();

		harness.pollLast();

		harness.getFirst();

		harness.getLast();

		harness.peekFirst();

		harness.peekLast();

		// CLEAR
		harness.clear();

		harness.peekFirst();

		harness.peekLast();

		harness.add("oak");

		harness.add("pine");

		harness.add("cherry");

		harness.add("maple");

		harness.add("pine");

		harness.add("maple");

		harness.add("birch");

		harness.add("balsa");

		harness.add("balsa");

		harness.removeFirstOccurrence("pine");

		harness.removeLastOccurrence("balsa");

		harness.removeFirstOccurrence("pear");

		harness.removeLastOccurrence("apple");

		harness.remove();

		harness.poll();

		harness.element();

		harness.peek();

		// CLEAR
		harness.poll();

		harness.peek();

		harness.push("oak");

		harness.push("pine");

		harness.push("cherry");

		harness.push("maple");

		harness.push("birch");

		harness.push("balsa");

		harness.pop();

		harness.addFirst("1");
		harness.addFirst("2");
		harness.addFirst("3");
		harness.addFirst("4");
		harness.addFirst("5");
		harness.addFirst("6");
		harness.addFirst("7");
		harness.addFirst("8");
		harness.addFirst("9");
		harness.addFirst("10");

		int i = 0;
		for (Iterator<String> it = harness.descendingIterator(); it.hasNext(); i++) {
			it.next();
			if (i % 2 == 0) {
				it.remove();
			}
		}

		harness.pop();

		System.out.println("passed");
	}

	public static void assert_(final boolean bool) {
		if (!bool) throw new RuntimeException();
	}

	public static class DequeHarness implements Deque<String> {

		@Override
		public boolean add(final String s) {
			// assumed correct
			linkedlist.add(s);
			return dlist.add(s);
		}

		@Override
		public void clear() {
			// assumed correct
			linkedlist.clear();
			dlist.clear();
		}

		@Override
		public void addFirst(final String s) {
			linkedlist.addFirst(s);
			dlist.addFirst(s);

			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}
		}

		@Override
		public void addLast(final String s) {
			linkedlist.addLast(s);
			dlist.addLast(s);

			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}
		}

		@Override
		public boolean offerFirst(final String s) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean offerLast(final String s) {
			throw new UnsupportedOperationException();
		}

		@Override
		public String removeFirst() {
			final String linkedresult = linkedlist.removeFirst();
			final String dresult = dlist.removeFirst();

			assert_(Objects.equals(linkedresult, dresult));
			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}

			return linkedresult;
		}

		@Override
		public String removeLast() {
			final String linkedresult = linkedlist.removeLast();
			final String dresult = dlist.removeLast();

			assert_(Objects.equals(linkedresult, dresult));
			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}

			return linkedresult;
		}

		@Override
		public String pollFirst() {
			final String linkedresult = linkedlist.pollFirst();
			final String dresult = dlist.pollFirst();

			assert_(Objects.equals(linkedresult, dresult));
			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}

			return linkedresult;
		}

		@Override
		public String pollLast() {
			final String linkedresult = linkedlist.pollLast();
			final String dresult = dlist.pollLast();

			assert_(Objects.equals(linkedresult, dresult));
			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}

			return linkedresult;
		}

		@Override
		public String getFirst() {
			final String linkedresult = linkedlist.getFirst();
			final String dresult = dlist.getFirst();

			assert_(Objects.equals(linkedresult, dresult));
			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}

			return linkedresult;
		}

		@Override
		public String getLast() {
			final String linkedresult = linkedlist.getLast();
			final String dresult = dlist.getLast();

			assert_(Objects.equals(linkedresult, dresult));
			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}

			return linkedresult;
		}

		@Override
		public String peekFirst() {
			final String linkedresult = linkedlist.peekFirst();
			final String dresult = dlist.peekFirst();

			assert_(Objects.equals(linkedresult, dresult));
			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}

			return linkedresult;
		}

		@Override
		public String peekLast() {
			final String linkedresult = linkedlist.peekLast();
			final String dresult = dlist.peekLast();

			assert_(Objects.equals(linkedresult, dresult));
			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}

			return linkedresult;
		}

		@Override
		public boolean removeFirstOccurrence(final Object o) {
			final boolean linkedresult = linkedlist.removeFirstOccurrence(o);
			final boolean dresult = dlist.removeFirstOccurrence(o);

			assert_(linkedresult == dresult);
			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}

			return linkedresult;
		}

		@Override
		public boolean removeLastOccurrence(final Object o) {
			final boolean linkedresult = linkedlist.removeLastOccurrence(o);
			final boolean dresult = dlist.removeLastOccurrence(o);

			assert_(linkedresult == dresult);
			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}

			return linkedresult;
		}

		@Override
		public boolean offer(final String s) {
			throw new UnsupportedOperationException();
		}

		@Override
		public String remove() {
			final String linkedresult = linkedlist.remove();
			final String dresult = dlist.remove();

			assert_(Objects.equals(linkedresult, dresult));
			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}

			return linkedresult;
		}

		@Override
		public String poll() {
			final String linkedresult = linkedlist.poll();
			final String dresult = dlist.poll();

			assert_(Objects.equals(linkedresult, dresult));
			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}

			return linkedresult;
		}

		@Override
		public String element() {
			final String linkedresult = linkedlist.element();
			final String dresult = dlist.element();

			assert_(Objects.equals(linkedresult, dresult));
			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}

			return linkedresult;
		}

		@Override
		public String peek() {
			final String linkedresult = linkedlist.peek();
			final String dresult = dlist.peek();

			assert_(Objects.equals(linkedresult, dresult));
			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}

			return linkedresult;
		}

		@Override
		public void push(final String s) {
			linkedlist.push(s);
			dlist.push(s);

			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}
		}

		@Override
		public String pop() {
			final String linkedresult = linkedlist.pop();
			final String dresult = dlist.pop();

			assert_(Objects.equals(linkedresult, dresult));
			assert_(linkedlist.equals(dlist));
			assert_(dlist.equals(linkedlist));

			final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
			final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

			if (end < start) {
				assert_(dlist.MAX_HEIGHT - start <= size());
				assert_(end <= size());
			}

			return linkedresult;
		}

		@Override
		public boolean remove(final Object o) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean containsAll(final Collection<?> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean addAll(final Collection<? extends String> c) {
			throw new UnsupportedOperationException();
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
		public boolean contains(final Object o) {
			throw new UnsupportedOperationException();
		}

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
		public @NotNull Iterator<String> iterator() {
			throw new UnsupportedOperationException();
		}

		@Override
		public @NotNull Object[] toArray() {
			throw new UnsupportedOperationException();
		}

		@Override
		public @NotNull <T> T[] toArray(final T[] a) {
			throw new UnsupportedOperationException();
		}

		@Override
		public @NotNull Iterator<String> descendingIterator() {
			final Iterator<String> linkedresult = linkedlist.descendingIterator();
			final Iterator<String> dresult = dlist.descendingIterator();

			return new Iterator<String>() {
				@Override
				public boolean hasNext() {
					final boolean linkedresult2 = linkedresult.hasNext();
					final boolean dresult2 = dresult.hasNext();

					assert_(linkedresult2 == dresult2);

					assert_(linkedlist.equals(dlist));
					assert_(dlist.equals(linkedlist));

					final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
					final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

					if (end < start) {
						assert_(dlist.MAX_HEIGHT - start <= size());
						assert_(end <= size());
					}

					return linkedresult2;
				}

				@Override
				public String next() {
					final String linkedresult2 = linkedresult.next();
					final String dresult2 = dresult.next();

					assert_(Objects.equals(linkedresult2, dresult2));

					assert_(linkedlist.equals(dlist));
					assert_(dlist.equals(linkedlist));

					final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
					final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

					if (end < start) {
						assert_(dlist.MAX_HEIGHT - start <= size());
						assert_(end <= size());
					}

					return linkedresult2;
				}

				@Override
				public void remove() {
					linkedresult.remove();
					dresult.remove();

					assert_(linkedlist.equals(dlist));
					assert_(dlist.equals(linkedlist));

					final int start = util.new IntUnion(dlist.kv.read(util.new IntUnion(-2).toBA())).getInt();
					final int end = util.new IntUnion(dlist.kv.read(util.new IntUnion(-3).toBA())).getInt();

					if (end < start) {
						assert_(dlist.MAX_HEIGHT - start <= size());
						assert_(end <= size());
					}
				}
			};
		}
	}

}
