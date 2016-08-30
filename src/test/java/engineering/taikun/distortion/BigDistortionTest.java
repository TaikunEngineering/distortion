package engineering.taikun.distortion;

import engineering.taikun.distortion.ai.api.DistortionAI;
import engineering.taikun.distortion.ai.api.DistortionAI.Identifier;
import engineering.taikun.distortion.ai.imp.SimpleAI;
import engineering.taikun.distortion.api.fn.AdvancedFunction;
import engineering.taikun.distortion.api.fn.DistortionFunction;
import engineering.taikun.distortion.serialization.imp.ArrayWrapper;
import engineering.taikun.distortion.serialization.imp.ArrayWrapperFactory;
import engineering.taikun.distortion.store.imp.InMemoryHistoryKV;
import engineering.taikun.distortion.store.imp.StackToken;
import engineering.taikun.distortion.store.util.DistortionStoreShim;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class BigDistortionTest {

	// use Mathematica or WolframAlpha
	// Table[Round[100*(1-e^(-x))], {x, 0, 5, 0.01}]
	public static int[] e_distribution = {
			 0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 10, 11, 12, 13, 14, 15, 16, 16, 17, 18, 19, 20, 21, 21, 22, 23, 24,
			24, 25, 26, 27, 27, 28, 29, 30, 30, 31, 32, 32, 33, 34, 34, 35, 36, 36, 37, 37, 38, 39, 39, 40, 41, 41, 42, 42,
			43, 43, 44, 45, 45, 46, 46, 47, 47, 48, 48, 49, 49, 50, 50, 51, 51, 52, 52, 53, 53, 54, 54, 55, 55, 56, 56, 56,
			57, 57, 58, 58, 59, 59, 59, 60, 60, 61, 61, 61, 62, 62, 62, 63, 63, 64, 64, 64, 65, 65, 65, 66, 66, 66, 67, 67,
			67, 68, 68, 68, 69, 69, 69, 70, 70, 70, 70, 71, 71, 71, 72, 72, 72, 72, 73, 73, 73, 74, 74, 74, 74, 75, 75, 75,
			75, 76, 76, 76, 76, 77, 77, 77, 77, 77, 78, 78, 78, 78, 79, 79, 79, 79, 79, 80, 80, 80, 80, 80, 81, 81, 81, 81,
			81, 82, 82, 82, 82, 82, 82, 83, 83, 83, 83, 83, 83, 84, 84, 84, 84, 84, 84, 85, 85, 85, 85, 85, 85, 85, 86, 86,
			86, 86, 86, 86, 86, 87, 87, 87, 87, 87, 87, 87, 88, 88, 88, 88, 88, 88, 88, 88, 88, 89, 89, 89, 89, 89, 89, 89,
			89, 89, 90, 90, 90, 90, 90, 90, 90, 90, 90, 90, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 92, 92, 92, 92, 92,
			92, 92, 92, 92, 92, 92, 92, 92, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 94, 94, 94, 94, 94, 94,
			94, 94, 94, 94, 94, 94, 94, 94, 94, 94, 94, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95,
			95, 95, 95, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96,
			97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97,
			97, 97, 97, 97, 97, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98,
			98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98,
			99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99,
			99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99,
			99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99
	};

	public static int ITER_COUNT = 300;
	public static final int WORK_LENGTH = 50;

	@Test
	public static void main() throws InterruptedException, ExecutionException {

		System.out.println("Big Distortion test");

//		Thread.sleep(20_000);

		final DistortionStoreShim<ArrayWrapper, StackToken> store = new DistortionStoreShim<>(
				ArrayWrapper.UTIL, new StackToken(), InMemoryHistoryKV::new, null
		);


		final SerializationUtil<ArrayWrapper> util = new SerializationUtil<>(new ArrayWrapperFactory(), (short) 32);

		final DistortionAI ai = new SimpleAI();

		final Distortion distortion = new Distortion<>(store, ai, util, 8);

		final Logger logger = Logger.getAnonymousLogger();

		if (logger.getParent().getHandlers().length > 0)
			logger.getParent().removeHandler(logger.getParent().getHandlers()[0]);
		logger.addHandler(
				new Handler() {
					final long now = System.currentTimeMillis();
					@Override public void publish(final LogRecord record) {
						System.out.format("%7d -- %s\n", record.getMillis() - this.now, record.getMessage());
					}
					@Override public void flush() {}
					@Override public void close() throws SecurityException {}
				}
		);

		final AtomicInteger invocationCount = new AtomicInteger(0);

		//	HashMap<Integer, Integer> checker = new HashMap<>();

		logger.log(Level.INFO, "Running");

//		for (int i = 0; i < 100; i++) {
//			checker.put(i, 0);
//		}

		distortion.transform(
				new AdvancedFunction() {
					@Override public Collection<Identifier> getIdentifiers() {
						return Collections.singleton(new Identifier("distortion_test", "init"));
					}

					@Override public void transform(final Map persistent, final Map ephemeral) {
						for (int i = 0; i < 100; i++) {
							ephemeral.put(Integer.toString(i), 0);
						}

						doProtected(
								new DistortionFunction() {
									@Override public Collection<Identifier> getIdentifiers() {
										return Collections.singleton(new Identifier("distortion_test", "init", "report"));
									}

									@Override public void transform(final Map persistent, final Map ephemeral) {
										logger.log(Level.INFO, "Init complete");
									}
								}
						);
					}
				}
		).get();

		final Collection<Identifier>[] identifiers = new Collection[100];

		for (int i = 0; i < 100; i++) {
			identifiers[i] = Collections.singleton(new Identifier("tuning", Integer.toString(i)));
		}

		final Collection<Identifier> branchingIdentifier = Collections.singleton(new Identifier("tuning", "branching"));

		final Random random = new Random(324);

		invocationCount.addAndGet(ITER_COUNT * 4 * WORK_LENGTH);

		for (int i = 0; i < ITER_COUNT; i++) {
			final int one = e_distribution[random.nextInt(e_distribution.length)];
			final int two = e_distribution[random.nextInt(e_distribution.length)];
			final int three = e_distribution[random.nextInt(e_distribution.length)];

//			checker.put(one, checker.get(one) + 2);
//			checker.put(two, checker.get(two) + 1);
//			checker.put(three, checker.get(three) + 1);

			distortion.transform(
					new DistortionFunction() {
						@Override public Collection<Identifier> getIdentifiers() {
							return identifiers[one];
						}

						@Override public void transform(final Map persistent, final Map ephemeral) {
							sleep(WORK_LENGTH);
							ephemeral.put(Integer.toString(one), (Integer) ephemeral.get(Integer.toString(one)) + 1);
						}
					}
			);

			distortion.transform(
					new DistortionFunction() {
						@Override public Collection<Identifier> getIdentifiers() {
							return identifiers[two];
						}

						@Override public void transform(final Map persistent, final Map ephemeral) {
							sleep(WORK_LENGTH);
							ephemeral.put(Integer.toString(two), (Integer) ephemeral.get(Integer.toString(two)) + 1);
						}
					}
			);

			distortion.transform(
					new AdvancedFunction() {
						@Override public Collection<Identifier> getIdentifiers() {
							return branchingIdentifier;
						}

						@Override public void transform(final Map persistent, final Map ephemeral) {
							sleep(WORK_LENGTH);
							branch(
									new DistortionFunction() {
										@Override public Collection<Identifier> getIdentifiers() {
											return identifiers[one];
										}

										@Override public void transform(final Map persistent, final Map ephemeral) {
											sleep(WORK_LENGTH);
											ephemeral.put(Integer.toString(one), (Integer) ephemeral.get(Integer.toString(one)) + 1);
										}
									}
							);
							branch(
									new DistortionFunction() {
										@Override public Collection<Identifier> getIdentifiers() {
											return identifiers[three];
										}

										@Override public void transform(final Map persistent, final Map ephemeral) {
											sleep(WORK_LENGTH);
											ephemeral.put(Integer.toString(three), (Integer) ephemeral.get(Integer.toString(three)) + 1);
										}
									}
							);
						}
					}
			);

			sleep(100);
		}

		final String[] capture = new String[1];

		distortion.transform(
				new AdvancedFunction() {
					@Override public Collection<Identifier> getIdentifiers() {
						return Collections.singleton(new Identifier("distortion_test", "report", "prep"));
					}

					@Override public void transform(final Map persistent, final Map ephemeral) {
						doProtected(
								new DistortionFunction() {
									@Override public Collection<Identifier> getIdentifiers() {
										return Collections.singleton(new Identifier("distortion_test", "report", "print"));
									}

									@Override public void transform(final Map persistent, final Map ephemeral) {
										logger.log(Level.INFO, "------");
										ephemeral.entrySet().forEach(
												o -> {
													Map.Entry entry = (Map.Entry) o;
													logger.log(Level.INFO, entry.toString());
												}
										);
										logger.log(Level.INFO, "------");

//										System.out.println(ephemeral.values().stream().mapToInt(value -> (Integer) value).sum());

										capture[0] = ephemeral.toString();
									}
								}
						);
					}
				}
		).get();

		System.out.println("Pool size -- " + distortion.getStats().get("pool size"));

		distortion.shutdown();

		System.out.println(distortion.getStats());

		System.out.println(invocationCount.get());

		assert_(capture[0].equals("{0=3, 1=1, 2=2, 3=5, 4=0, 5=1, 6=1, 7=0, 8=0, 9=4, 10=9, 11=4, 12=2, 13=1, 14=2, 15=2, 16=3, 17=5, 18=2, 19=0, 20=2, 21=1, 22=1, 23=3, 24=7, 25=4, 26=0, 27=2, 28=4, 29=0, 30=5, 31=6, 32=2, 33=7, 34=2, 35=0, 36=2, 37=0, 38=2, 39=7, 40=0, 41=2, 42=4, 43=2, 44=3, 45=4, 46=2, 47=2, 48=4, 49=2, 50=3, 51=6, 52=3, 53=5, 54=3, 55=7, 56=2, 57=4, 58=5, 59=6, 60=3, 61=8, 62=6, 63=8, 64=2, 65=9, 66=8, 67=5, 68=13, 69=4, 70=8, 71=9, 72=10, 73=15, 74=16, 75=12, 76=15, 77=16, 78=11, 79=10, 80=13, 81=12, 82=22, 83=11, 84=18, 85=22, 86=20, 87=22, 88=13, 89=23, 90=23, 91=20, 92=28, 93=32, 94=41, 95=53, 96=48, 97=86, 98=118, 99=214}"));

		System.out.println("passed");
	}

	public static void assert_(final boolean bool) {
		if (!bool) throw new RuntimeException();
	}

	public static void sleep(final long millis) {
		try {
			Thread.sleep(millis);
		} catch (final InterruptedException e) {}
	}

}
