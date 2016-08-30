package engineering.taikun.distortion;

import engineering.taikun.distortion.ai.api.DistortionAI.Identifier;
import engineering.taikun.distortion.ai.api.DistortionAI.Prediction;
import engineering.taikun.distortion.ai.imp.StaticAI;
import engineering.taikun.distortion.ai.imp.StaticAI.StaticAIBuilder;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StaticAITest {

	@Test
	public static void main() {

		System.out.println("Static AI test");

		final StaticAIBuilder builder = new StaticAIBuilder();

		builder.add("static", "one").dependsOn("static", "two");

		builder.add(
				Collections.singletonList(new String[]{ "static", "three"}),
				Collections.singletonList(new String[]{ "static", "four"})
		);

		builder.add("static", "five").dependsOn("static", "six");
		builder.add("static", "five").dependsOn("static", "seven");

		builder.add("star", "A", "*").dependsOn("star", "A", "*");
		builder.add("star", "A", "*").dependsOn("star", "B", "*");
		builder.add("star", "A", "*").dependsOn("star", "static", "one");
		builder.add("star", "A", "*").dependsOn();

		builder.add("bar", "A", "_").dependsOn("bar", "B", "_");
		builder.add("bar", "A", "_").dependsOn("bar", "C", "_");

		final StaticAI ai = builder.build();

		final ArrayList<Collection<Identifier>> state = new ArrayList<>();

		// static testing

		state.add(Collections.singleton(new Identifier("static", "zero")));

		assertMatch_(ai.predictHelper(state,
				Collections.singleton(new Identifier("static", "one"))),
				false
		);

		state.add(Collections.singleton(new Identifier("static", "two")));

		assertMatch_(ai.predictHelper(state,
				Collections.singleton(new Identifier("static", "one"))),
				false, true
		);

		assertMatch_(ai.predictHelper(state,
				Arrays.asList(new Identifier("static", "one"), new Identifier("static", "one"))),
				false, true
		);

		assertMatch_(ai.predictHelper(state,
				Collections.singleton(new Identifier("static", "zero"))),
				false, false
		);

		assertMatch_(ai.predictHelper(state,
				Collections.singleton(new Identifier("star", "A"))),
				false, false
		);

		// star testing

		state.add(Collections.singleton(new Identifier("star", "A", "one")));

		assertMatch_(ai.predictHelper(state,
				Collections.singleton(new Identifier("star", "A", "two"))),
				false, false, true
		);

		assertMatch_(ai.predictHelper(state,
				Arrays.asList(new Identifier("star", "A", "two"), new Identifier("star", "A", "two"))),
				false, false, true
		);

		state.add(Collections.singleton(new Identifier("star", "B", "two")));

		assertMatch_(ai.predictHelper(state,
				Collections.singleton(new Identifier("star", "A", "three"))),
				false, false, true, true
		);

		state.add(Collections.singleton(new Identifier("star", "static", "one")));

		assertMatch_(ai.predictHelper(state,
				Collections.singleton(new Identifier("star", "A", "three"))),
				false, false, true, true, true
		);

		state.add(Collections.singleton(new Identifier()));

		assertMatch_(ai.predictHelper(state,
				Collections.singleton(new Identifier("star", "A", "three"))),
				false, false, true, true, true, true
		);

		// bar testing

		state.add(Collections.singleton(new Identifier("bar", "B", "one")));
		state.add(Collections.singleton(new Identifier("bar", "C", "one")));

		assertMatch_(ai.predictHelper(state,
				Collections.singleton(new Identifier("bar", "A", "one"))),
				false, false, false, false, false, false, true, true
		);

		assertMatch_(ai.predictHelper(state,
				Collections.singleton(new Identifier("bar", "A"))),
				false, false, false, false, false, false, false, false
		);

		// error testing

		try {
			final StaticAIBuilder temp = new StaticAIBuilder();

			temp.add("illegal", "*", "placement").dependsOn();

			temp.build();

			throw new RuntimeException("Failed to throw exception");
		} catch (final IllegalArgumentException ignored) {}

		try {
			final StaticAIBuilder temp = new StaticAIBuilder();

			temp.add("some", "thing", "_").dependsOn();

			temp.build();

			throw new RuntimeException("Failed to throw exception");
		} catch (final IllegalArgumentException ignored) {}

		try {
			final StaticAIBuilder temp = new StaticAIBuilder();

			temp.add("some", "thing", "_").dependsOn("wrong");

			temp.build();

			throw new RuntimeException("Failed to throw exception");
		} catch (final IllegalArgumentException ignored) {}

		System.out.println("passed");

	}

	public static Collection<Collection<Identifier>> stateOf(final Identifier... identifiers) {
		return Stream.of(identifiers).map(Collections::singleton).collect(Collectors.toList());
	}

	public static void assert_(final boolean bool) {
		if (!bool) throw new RuntimeException();
	}

	public static void assertMatch_(final Prediction prediction, final boolean... booleans) {
		assert_(Arrays.equals(prediction.flags, booleans));
	}

}
