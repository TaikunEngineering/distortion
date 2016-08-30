package engineering.taikun.distortion;

import engineering.taikun.distortion.ai.api.DistortionAI;
import engineering.taikun.distortion.ai.api.DistortionAI.Identifier;
import engineering.taikun.distortion.ai.imp.StaticAI.StaticAIBuilder;
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
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class DistortionTest {

	static DistortionStoreShim<ArrayWrapper, StackToken> store = new DistortionStoreShim<>(
			ArrayWrapper.UTIL, new StackToken(), InMemoryHistoryKV::new, null
	);

	static SerializationUtil<ArrayWrapper> util = new SerializationUtil<>(new ArrayWrapperFactory(), (short) 32);

//	static DistortionAI ai = new SimpleAI();
//	static DistortionAI ai = new OptimisticAI();
//	static DistortionAI ai = new FineOptimisticAI();
	static DistortionAI ai;
	static {
		StaticAIBuilder builder = new StaticAIBuilder();

		builder.add("distortion_test", "A", "*").dependsOn("distortion_test", "A", "*");
		builder.add("distortion_test", "B", "*").dependsOn("distortion_test", "B", "*");

		ai = builder.build();
	}

	static Distortion distortion = new Distortion<>(store, ai, util, 4);

	static Logger logger = Logger.getAnonymousLogger();

	static {
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
	}

	static AtomicInteger invocationCount = new AtomicInteger(0);

	public static final int WORK_LENGTH = 50;

	@Test
	public static void main() throws InterruptedException, ExecutionException {

		System.out.println("Distortion test");

		logger.log(Level.INFO, "Running");

		final Future initFuture = distortion.transform(
				new AdvancedFunction() {
					@Override public Collection<Identifier> getIdentifiers() {
						return Collections.singleton(new Identifier("distortion_test", "init"));
					}

					@Override public void transform(final Map persistent, final Map ephemeral) {
						ephemeral.put("A", 0);
						ephemeral.put("B", 0);

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
		);

		initFuture.get();

		final Random random = new Random(324);

		for (int i = 0; i < 200; i++) {
			final int r_val = random.nextInt(4);

			if (r_val == 0) {
				doA();
				doA();
			} else if (r_val == 1) {
				doB();
				doB();
			} else if (r_val == 2) {
				doA();
				doB();
				doC();
			} else if (r_val == 3) {
				doB();
				doA();
				doC();
			}

			sleep(2 * WORK_LENGTH);
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

		assert_(capture[0].equals("{A=298, B=302}"));

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

	public static Future<?> doA() throws InterruptedException {
		invocationCount.addAndGet(WORK_LENGTH);
		return distortion.transform(
				new AdvancedFunction() {
					@Override public Collection<Identifier> getIdentifiers() {
						return Collections.singleton(new Identifier("distortion_test", "A", "increment"));
					}

					@Override public void transform(final Map persistent, final Map ephemeral) {
						sleep(WORK_LENGTH);
						ephemeral.put("A", (Integer) ephemeral.get("A") + 1);

						doProtected(
								new DistortionFunction() {
									@Override public Collection<Identifier> getIdentifiers() {
										return Collections.singleton(new Identifier("distortion_test", "A", "report"));
									}

									@Override public void transform(final Map persistent, final Map ephemeral) {
										logger.log(Level.INFO, "A: " + ephemeral.get("A"));
									}
								}
						);
					}
				}
		);
	}

	public static Future<?> doB() throws InterruptedException {
		invocationCount.addAndGet(WORK_LENGTH);
		return distortion.transform(
				new AdvancedFunction() {
					@Override public Collection<Identifier> getIdentifiers() {
						return Collections.singleton(new Identifier("distortion_test", "B", "increment"));
					}

					@Override public void transform(final Map persistent, final Map ephemeral) {
						sleep(WORK_LENGTH);
						ephemeral.put("B", (Integer) ephemeral.get("B") + 1);

						doProtected(
								new DistortionFunction() {
									@Override public Collection<Identifier> getIdentifiers() {
										return Collections.singleton(new Identifier("distortion_test", "B", "report"));
									}

									@Override public void transform(final Map persistent, final Map ephemeral) {
										logger.log(Level.INFO, "B: " + ephemeral.get("B"));
									}
								}
						);
					}
				}
		);
	}

	public static Future<?> doC() throws InterruptedException {
		invocationCount.addAndGet(WORK_LENGTH * 3);
		return distortion.transform(
				new AdvancedFunction() {
					@Override public Collection<Identifier> getIdentifiers() {
						return Collections.singleton(new Identifier("distortion_test", "C", "branching"));
					}

					@Override public void transform(final Map persistent, final Map ephemeral) {
						sleep(WORK_LENGTH);

						branch(
								new AdvancedFunction() {
									@Override public Collection<Identifier> getIdentifiers() {
										return Collections.singleton(new Identifier("distortion_test", "A", "increment"));
									}

									@Override public void transform(final Map persistent, final Map ephemeral) {
										sleep(WORK_LENGTH);
										ephemeral.put("A", (Integer) ephemeral.get("A") + 1);

										doProtected(
												new DistortionFunction() {
													@Override public Collection<Identifier> getIdentifiers() {
														return Collections.singleton(new Identifier("distortion_test", "A", "report"));
													}

													@Override public void transform(final Map persistent, final Map ephemeral) {
														logger.log(Level.INFO, "A: " + ephemeral.get("A"));
													}
												}
										);
									}
								}
						);

						branch(
								new AdvancedFunction() {
									@Override public Collection<Identifier> getIdentifiers() {
										return Collections.singleton(new Identifier("distortion_test", "B", "increment"));
									}

									@Override public void transform(final Map persistent, final Map ephemeral) {
										sleep(WORK_LENGTH);
										ephemeral.put("B", (Integer) ephemeral.get("B") + 1);

										doProtected(
												new DistortionFunction() {
													@Override public Collection<Identifier> getIdentifiers() {
														return Collections.singleton(new Identifier("distortion_test", "B", "report"));
													}

													@Override public void transform(final Map persistent, final Map ephemeral) {
														logger.log(Level.INFO, "B: " + ephemeral.get("B"));
													}
												}
										);
									}
								}
						);
					}
				}
		);
	}

}
