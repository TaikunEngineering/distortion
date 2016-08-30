package engineering.taikun.distortion.store.imp;

import engineering.taikun.distortion.store.api.DrillingToken;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * <p>A {@link DrillingToken} implementation using a stack</p>
 *
 * <p>Not the greatest, but token performance barely matters</p>
 */
public class StackToken implements DrillingToken<StackToken> {

	public final long root;
	public final @Nullable int[] chain;

	public StackToken() {
		this.root = 0L;
		this.chain = null;
	}

	private StackToken(final long root, final @Nullable int[] chain) {
		this.root = root;
		this.chain = chain;
	}

	@Override
	public StackToken getNextToken() {
		if (this.chain == null) {
			return new StackToken(this.root + 1, null);
		}

		if (this.chain[this.chain.length - 1] == Integer.MAX_VALUE)
			throw new StackOverflowError("StackToken chain-value overflow");

		final int[] newchain = Arrays.copyOf(this.chain, this.chain.length);
		newchain[this.chain.length - 1] = this.chain[this.chain.length - 1] + 1;

		return new StackToken(this.root, newchain);
	}

	@Override
	public StackToken getDeepToken() {
		if (this.chain == null) {
			return new StackToken(this.root, new int[]{ 0 });
		}

		final int[] newchain = Arrays.copyOf(this.chain, this.chain.length + 1);
		newchain[this.chain.length] = 0;

		return new StackToken(this.root, newchain);
	}

	@Override
	public int compareTo(final @NotNull StackToken that) {
		if (this.root != that.root) {
			return Long.compare(this.root, that.root);
		}

		// roots equal

		if (this.chain == null && that.chain == null) {
			return 0;
		}

		if (this.chain != null && that.chain == null) {
			return 1;
		}

		if (this.chain == null) { // that.chain != null
			return -1;
		}

		// chains not null

		for (int i = 0; i < this.chain.length && i < that.chain.length; i++) {
			final int compare = Integer.compare(this.chain[i], that.chain[i]);

			if (compare != 0) {
				return compare;
			}
		}

		// fallthrough means the subchains are identical
		// longer chain is greater

		return Integer.compare(this.chain.length, that.chain.length);
	}

	public static int compare(final @NotNull StackToken a, final @NotNull StackToken b) {
		return a.compareTo(b);
	}

	@Override
	public boolean equals(final Object o) {
		return o instanceof StackToken && this.compareTo((StackToken) o) == 0;
	}

	@Override
	public String toString() {

		final StringBuilder sb = new StringBuilder();

		sb.append('[');
		sb.append(this.root);

		if (this.chain != null) {
			for (final int value : this.chain) {
				sb.append(", ");
				sb.append(value);
			}
		}

		sb.append(']');

		return sb.toString();
	}

}
