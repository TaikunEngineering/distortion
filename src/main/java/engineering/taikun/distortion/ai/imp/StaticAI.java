package engineering.taikun.distortion.ai.imp;

import engineering.taikun.distortion.ai.api.DistortionAI;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Stream;

public final class StaticAI implements DistortionAI {

	final Node root = new Node();

	StaticAI(final ArrayList<String[]> functions, final ArrayList<String[]> dependencies) {

		Stream.concat(functions.stream(), dependencies.stream()).forEach(ar -> {
			for (int i = 0; i < ar.length; i++) {
				if ((ar[i].equals("_") || ar[i].equals("*")) && i != ar.length - 1) {
					throw new IllegalArgumentException("'_' and '*' must be the last token");
				}
			}
		});

	bigloop:
		for (int i = 0; i < functions.size(); i++) {

			Node node = this.root;

			final String[] stringar = functions.get(i);
			for (final String string : stringar) {
				switch (string) {
					case "_": {

						final String[] toinsert;
						final String[] dep = dependencies.get(i);
						if (dep.length > 0 && dep[dep.length - 1].equals("_")) {
							toinsert = Arrays.copyOf(dep, dep.length - 1);
						} else {
							throw new IllegalArgumentException("'_' functions must depend on '_' functions, use '*' for wildcard matching");
						}

						if (node.bar == null) {
							node.bar = new String[][]{ toinsert };
						} else {
							node.bar = Arrays.copyOf(node.bar, node.bar.length + 1);
							node.bar[node.bar.length - 1] = toinsert;
						}
						continue bigloop;
					}
					case "*": {

						final Object toinsert;
						final String[] dep = dependencies.get(i);
						if (dep.length > 0 && dep[dep.length - 1].equals("*")) {
							toinsert = Arrays.copyOf(dep, dep.length - 1);
						} else {
							toinsert = new Identifier(dep);
						}

						if (node.star == null) {
							node.star = new Object[]{ toinsert };
						} else {
							node.star = Arrays.copyOf(node.star, node.star.length + 1);
							node.star[node.star.length - 1] = toinsert;
						}
						continue bigloop;
					}
					default:

						if (node.map == null) {
							node.map = new HashMap<>();
						}

						final Node fetch = node.map.get(string);

						if (fetch == null) {
							final Node new_node = new Node();
							node.map.put(string, new_node);
							node = new_node;
						} else {
							node = fetch;
						}
						break;
				}
			}

			if (node.empty == null) {

				node.empty = new Identifier[]{ new Identifier(dependencies.get(i)) };

			} else {

				node.empty = Arrays.copyOf(node.empty, node.empty.length + 1);
				node.empty[node.empty.length - 1] = new Identifier(dependencies.get(i));

			}
		}
	}

	@Override
	public ExecutionMode predict(
			final Collection<Collection<Identifier>> state, final Collection<Identifier> identifiers, final boolean[] flags
	) {
	bigloop:
		for (final Identifier iden : identifiers) {

			Node node = this.root;

			for (int i = 0; i < iden.strings.length; i++) {

				if (node.bar != null) {
					for (final String[] dep : node.bar) {

						// flag for [bar] + i..n

						// have [a, b, c, d]

						// function [a, b, _]
						// --> [x, y, _]

						// seek [x, y, c, d]

						final String[] test = Arrays.copyOf(dep, dep.length + iden.strings.length - i);

						int cursor = dep.length;
						for (int j = i; j < iden.strings.length; j++) {
							test[cursor++] = iden.strings[j];
						}

						flagExact(state, new Identifier(test), flags);
					}
				}

				if (node.star != null) {
					for (final Object dep : node.star) {

						if (dep instanceof Identifier) {
							flagExact(state, (Identifier) dep, flags);
						} else {

							// flag for prefix [star]

							flagPrefix(state, (String[]) dep, flags);
						}
					}
				}

				if (node.map == null) {
					continue bigloop;
				}

				node = node.map.get(iden.strings[i]);

				if (node == null) {
					continue bigloop;
				}
			}

			if (node.empty == null) {
				continue;
			}

			for (final Identifier test : node.empty) {
				flagExact(state, test, flags);
			}
		}

		return ExecutionMode.FINE_BLOCK;
	}

	private static void flagExact(
			final Collection<Collection<Identifier>> state, final Identifier find, final boolean[] flags
	) {
		int i = 0;
		for (final Collection<Identifier> pending : state) {
			if (!flags[i]) {

				flags[i] = pending.stream().filter(find::equals).findAny().isPresent();

			}
			i++;
		}
	}

	private static void flagPrefix(
			final Collection<Collection<Identifier>> state, final String[] prefix, final boolean[] flags
	) {
		int i = 0;
		for (final Collection<Identifier> pending : state) {
			if (!flags[i]) {

				flags[i] = pending.stream().filter(iden -> {
					for (int j = 0; j < prefix.length; j++) {
						if (j >= iden.strings.length)
							return false;
						if (!iden.strings[j].equals(prefix[j]))
							return false;
					}
					return true;
				}).findAny().isPresent();

			}
			i++;
		}
	}

	public static class Node {

		public @Nullable Identifier[] empty = null;
		public @Nullable Object[] star = null;
		public @Nullable String[][] bar = null;

		public @Nullable HashMap<String, Node> map = null;

	}

	public static final class StaticAIBuilder {

		final ArrayList<String[]> functions = new ArrayList<>();
		final ArrayList<String[]> dependencies = new ArrayList<>();

		public Dependo add(final String... args) {
			return new Dependo(args);
		}

		public void add(final List<String[]> functions, final List<String[]> dependencies) {
			this.functions.addAll(functions);
			this.dependencies.addAll(dependencies);
		}

		public StaticAI build() {
			return new StaticAI(this.functions, this.dependencies);
		}

		public final class Dependo {

			final String[] function;

			Dependo(final String[] function) {
				this.function = function;
			}

			public void dependsOn(final String... args) {
				StaticAIBuilder.this.functions.add(this.function);
				StaticAIBuilder.this.dependencies.add(args);
			}
		}
	}
}
