package engineering.taikun.distortion;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;

public class ThroughputSingleThread {

	static LongAdder submit_count = new LongAdder();

	public static void main(final String[] args) throws ExecutionException, InterruptedException {

		final ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

		for (int i = 0; i < 1000; i++) {
			map.put(Integer.toString(i), Integer.toString(0));
		}

		new Thread(() -> {

			int iterations = 0;

			while (true) {

				long capture = submit_count.longValue();
				iterations++;

				System.out.println(capture / iterations);

				if (iterations == 60) {
					System.exit(0);
				}

				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {}
			}


		}).start();


		while (true) {

			final int index = ThreadLocalRandom.current().nextInt(1000);

			map.put(Integer.toString(index), Integer.toString(Integer.parseInt(map.get(Integer.toString(index))) + 1));

			submit_count.increment();
		}

	}

}