package engineering.taikun.distortion;

import engineering.taikun.distortion.ai.api.DistortionAI;
import engineering.taikun.distortion.ai.imp.SimpleAI;
import engineering.taikun.distortion.api.fn.OptimisticFunction;
import engineering.taikun.distortion.serialization.api.ByteArrayFactory;
import engineering.taikun.distortion.serialization.imp.ArrayWrapper;
import engineering.taikun.distortion.serialization.imp.ArrayWrapperFactory;
import engineering.taikun.distortion.store.imp.InMemoryHistoryKV;
import engineering.taikun.distortion.store.imp.StackToken;
import engineering.taikun.distortion.store.util.DistortionStoreShim;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;

public class ThroughputStresser {

	static LongAdder submit_count = new LongAdder();
	static LongAdder exec_count = new LongAdder();

	static boolean locked;

	public static void main(final String[] args) throws ExecutionException, InterruptedException {

		final DistortionStoreShim<ArrayWrapper, StackToken> store = new DistortionStoreShim<>(
				ArrayWrapper.UTIL, new StackToken(), InMemoryHistoryKV::new, null
		);

		final ByteArrayFactory<ArrayWrapper> factory = new ArrayWrapperFactory();
		final SerializationUtil<ArrayWrapper> util = new SerializationUtil<>(
				factory, (short) 32,
				b -> locked = b,
				() -> locked
		);

		final DistortionAI ai = new SimpleAI();

		final Distortion distortion = new Distortion<>(store, ai, util, 8);

		distortion.transform(new OptimisticFunction() {
			@Override public void transform(final Map persistent, final Map ephemeral) {
				for (int i = 0; i < 1000; i++) {
					ephemeral.put(i, 0);
				}
			}
		}).get();

		new Thread(() -> {

			int iterations = 0;

			while (true) {

				long capture = submit_count.longValue();
				long exec_cap = exec_count.longValue();
				iterations++;

				System.out.println("" + capture / iterations + " : " + exec_cap / iterations);

				if (iterations == 60) {
					System.exit(0);
				}

				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {}
			}


		}).start();

		while( true) {
			distortion.transform(randomIncrement);
			submit_count.increment();
		}

	}

	static OptimisticFunction randomIncrement = new MyOptimisticFunction();

	static class MyOptimisticFunction extends OptimisticFunction {
		@Override public void transform(final Map persistent, final Map ephemeral) {

			final int index = ThreadLocalRandom.current().nextInt(1000);

//			ephemeral.put(index, (Integer) ephemeral.get(index) + 1);
			ephemeral.get(index);

			exec_count.increment();

		}
	}
}
