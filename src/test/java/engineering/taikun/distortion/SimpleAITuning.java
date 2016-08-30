package engineering.taikun.distortion;

import engineering.taikun.distortion.ai.api.DistortionAI.Identifier;
import engineering.taikun.distortion.ai.imp.SimpleAI;
import engineering.taikun.distortion.api.fn.AdvancedFunction;
import engineering.taikun.distortion.api.fn.DistortionFunction;
import engineering.taikun.distortion.serialization.imp.ArrayWrapper;
import engineering.taikun.distortion.serialization.imp.ArrayWrapperFactory;
import engineering.taikun.distortion.store.imp.InMemoryHistoryKV;
import engineering.taikun.distortion.store.imp.StackToken;
import engineering.taikun.distortion.store.util.DistortionStoreShim;
import org.uncommons.maths.random.MersenneTwisterRNG;
import org.uncommons.watchmaker.framework.*;
import org.uncommons.watchmaker.framework.factories.AbstractCandidateFactory;
import org.uncommons.watchmaker.framework.operators.EvolutionPipeline;
import org.uncommons.watchmaker.framework.selection.RouletteWheelSelection;
import org.uncommons.watchmaker.framework.termination.GenerationCount;

import java.util.*;

public class SimpleAITuning {

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

	public static int WARMUP_COUNT = 200;
	public static int ITER_COUNT = 300;
	public static long WORK_LENGTH = 50;

	public static void main(final String[] args) {
		final CandidateFactory<SimpleAIParameters> parametersFactory = new AbstractCandidateFactory<SimpleAIParameters>() {
			@Override public SimpleAIParameters generateRandomCandidate(final Random rng) {
				final SimpleAIParameters toreturn = new SimpleAIParameters();

				for (int i = 1; i < 10; i++) {
					toreturn.params[i] = rng.nextDouble();
				}

				return toreturn;
			}
			};

			final EvolutionaryOperator<SimpleAIParameters> crossover = (selectedCandidates, rng) -> {
				final ArrayList<SimpleAIParameters> toreturn = new ArrayList<>(selectedCandidates.size());

				for (int i = 0; i < selectedCandidates.size(); i++) {
				final SimpleAIParameters temp = selectedCandidates.get(i).clone();

				final int index = rng.nextInt(9) + 1;

				temp.params[index] = selectedCandidates.get(rng.nextInt(selectedCandidates.size())).params[index];

				toreturn.add(temp);
			}

			return toreturn;
		};

		final EvolutionaryOperator<SimpleAIParameters> mutator = (selectedCandidates, rng) -> {
			final ArrayList<SimpleAIParameters> toreturn = new ArrayList<>(selectedCandidates.size());

			for (final SimpleAIParameters old : selectedCandidates) {
				final SimpleAIParameters temp = old.clone();

				final int index = rng.nextInt(9) + 1;

				double param = temp.params[index];

				param = 0.8 * param + 0.2 * (rng.nextDouble() - 0.5);

				param = Math.max(param, 0.0);
				param = Math.min(param, 1.0);

				temp.params[index] = param;

				toreturn.add(temp);
			}

			return toreturn;
		};

		final EvolutionaryOperator<SimpleAIParameters> pipeline = new EvolutionPipeline<>(Arrays.asList(crossover, mutator));

		final FitnessEvaluator<SimpleAIParameters> fitnessEvaluator = new FitnessEvaluator<SimpleAIParameters>() {
			@Override public double getFitness(
					final SimpleAIParameters candidate, final List<? extends SimpleAIParameters> population
			) {
				return evaluate(candidate);
			}

			@Override public boolean isNatural() {
				return false;
			}
		};

		final EvolutionObserver<SimpleAIParameters> evolutionObserver
				= data -> System.out.println("Gen " + data.getGenerationNumber() + " -- " + (long) data.getBestCandidateFitness() + "ns -- " + data.getBestCandidate());

		final GenerationalEvolutionEngine<SimpleAIParameters> engine = new GenerationalEvolutionEngine<SimpleAIParameters>(
				parametersFactory, pipeline, fitnessEvaluator, new RouletteWheelSelection(), new MersenneTwisterRNG()
		);

		engine.addEvolutionObserver(evolutionObserver);
		engine.setSingleThreaded(true);

		System.out.println(engine.evolve(100, 10, new GenerationCount(100)));
	}

	public static double evaluate(final SimpleAIParameters params) {
		try {

			final boolean[] locked = new boolean[1];

			final DistortionStoreShim<ArrayWrapper, StackToken> store = new DistortionStoreShim<>(
					ArrayWrapper.UTIL, new StackToken(), InMemoryHistoryKV::new, null
			);
			final SerializationUtil<ArrayWrapper> util = new SerializationUtil<>(
					new ArrayWrapperFactory(), (short) 32,
					b -> locked[0] = b,
					() -> locked[0]
			);
			final SimpleAI ai = new SimpleAI(params.params);

			final Distortion distortion = new Distortion<>(store, ai, util, 4);

			distortion.transform(
					new DistortionFunction() {
						@Override public Collection<Identifier> getIdentifiers() {
							return Collections.singleton(new Identifier("tuning", "init"));
						}

						@Override public void transform(final Map persistent, final Map ephemeral) {
							for (int i = 0; i < 100; i++) {
								ephemeral.put(Integer.toString(i), 0);
							}
						}
					}
			).get();

			final Collection<Identifier>[] identifiers = new Collection[100];

			for (int i = 0; i < 100; i++) {
				identifiers[i] = Collections.singleton(new Identifier("tuning", Integer.toString(i)));
			}

			final Collection<Identifier> branchingIdentifier = Collections.singleton(new Identifier("tuning", "branching"));

			final Random random = new Random(324);

			for (int i = 0; i < WARMUP_COUNT; i++) {
				final int one = e_distribution[random.nextInt(e_distribution.length)];
				final int two = e_distribution[random.nextInt(e_distribution.length)];
				final int three = e_distribution[random.nextInt(e_distribution.length)];

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
			}

			distortion.transform(
					new DistortionFunction() {
						@Override public Collection<Identifier> getIdentifiers() {
							return Collections.singleton(new Identifier("tuning", "pause"));
						}

						@Override public void transform(final Map persistent, final Map ephemeral) {
							// DO NOTHING
						}
					}
			).get();

			final long START = System.nanoTime();

			for (int i = 0; i < ITER_COUNT; i++) {
				final int one = e_distribution[random.nextInt(e_distribution.length)];
				final int two = e_distribution[random.nextInt(e_distribution.length)];
				final int three = e_distribution[random.nextInt(e_distribution.length)];

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
			}

			distortion.transform(
					new DistortionFunction() {
						@Override public Collection<Identifier> getIdentifiers() {
							return Collections.singleton(new Identifier("tuning", "pause"));
						}

						@Override public void transform(final Map persistent, final Map ephemeral) {
							// DO NOTHING
						}
					}
			).get();

			final long TIME = System.nanoTime() - START;

			final String[] capture = new String[1];

			distortion.transform(
					new DistortionFunction() {
						@Override public Collection<Identifier> getIdentifiers() {
							return Collections.singleton(new Identifier("correctness check"));
						}

						@Override public void transform(final Map persistent, final Map ephemeral) {
							capture[0] = ephemeral.toString();
						}
					}
			).get();

			distortion.shutdown();

			assert_(capture[0].equals("{0=3, 1=1, 2=3, 3=5, 4=3, 5=6, 6=1, 7=0, 8=0, 9=6, 10=17, 11=4, 12=2, 13=1, 14=3, 15=3, 16=5, 17=11, 18=4, 19=3, 20=3, 21=7, 22=1, 23=3, 24=11, 25=6, 26=1, 27=5, 28=6, 29=0, 30=5, 31=6, 32=2, 33=10, 34=2, 35=1, 36=3, 37=4, 38=3, 39=14, 40=2, 41=4, 42=9, 43=2, 44=8, 45=12, 46=6, 47=7, 48=8, 49=7, 50=3, 51=6, 52=6, 53=8, 54=4, 55=10, 56=7, 57=5, 58=6, 59=11, 60=6, 61=15, 62=18, 63=13, 64=9, 65=14, 66=13, 67=6, 68=23, 69=10, 70=12, 71=12, 72=15, 73=15, 74=24, 75=13, 76=16, 77=26, 78=16, 79=24, 80=28, 81=23, 82=30, 83=23, 84=29, 85=32, 86=26, 87=35, 88=35, 89=42, 90=39, 91=44, 92=38, 93=59, 94=57, 95=83, 96=86, 97=158, 98=192, 99=336}"));

			return TIME;

		} catch (final Throwable t) {
			System.out.println("BIG SHIT");
			t.printStackTrace();

			throw new RuntimeException(t);
		}
	}

	public static void assert_(final boolean bool) {
		if (!bool) throw new RuntimeException();
	}

	public static void sleep(final long millis) {
		try {
			Thread.sleep(millis);
		} catch (final InterruptedException e) {}
	}

	public static class SimpleAIParameters {

		static final SimpleAIParameters DEFAULT = new SimpleAIParameters();
		static {
			DEFAULT.params[1] = 0.250;
			DEFAULT.params[2] = 0.655;
			DEFAULT.params[3] = 0.569;
			DEFAULT.params[4] = 0.401;
			DEFAULT.params[5] = 0.884;
			DEFAULT.params[6] = 0.112;
			DEFAULT.params[7] = 0.333;
			DEFAULT.params[8] = 0.643;
			DEFAULT.params[9] = 0.669;
		}

		public double[] params = new double[10];

		public SimpleAIParameters() {
			this.params[0] = 20;
		}

		@Override
		public SimpleAIParameters clone() {
			final SimpleAIParameters toreturn = new SimpleAIParameters();

			toreturn.params = Arrays.copyOf(this.params, 10);

			return toreturn;
		}

		@Override
		public String toString() {
			return "SimpleAIParameters -- " + Arrays.toString(params);
		}

	}

}
